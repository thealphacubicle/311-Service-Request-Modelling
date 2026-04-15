import os
from pathlib import Path
import pandas as pd


# Paths
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = REPO_ROOT / "data" / "raw" / "311_2019_2024.parquet"
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data" / "processed"

RAW_DATA_PATH = Path(os.environ.get("RAW_DATA_PATH", DEFAULT_RAW))
PROCESSED_DIR = Path(os.environ.get("PROCESSED_DIR", DEFAULT_PROCESSED_DIR))


# Constants
MIN_TYPE_COUNT = 1_000

# Neighborhoods to drop entirely (city-wide catch-alls with no geographic meaning)
DROP_NEIGHBORHOODS = {"Boston", "Chestnut Hill"}

# Merge neighborhood labels into a single collective name
NEIGHBORHOOD_MERGE_MAP = {
    "Allston": "Allston / Brighton",
    "Brighton": "Allston / Brighton",
    "South Boston / South Boston Waterfront": "South Boston",
    "Greater Mattapan": "Mattapan",
}


# Step 1: Load & core cleaning  (shared by both models)
def load_and_clean(path: Path) -> pd.DataFrame:
    """
    Load the raw parquet and apply all cleaning that both models need:
    """
    print(f"Loading raw data from {path}")
    df = pd.read_parquet(path)

    # Parse datetime into proper Timestamp object
    df["open_dt"] = pd.to_datetime(df["open_dt"], errors="coerce")
    df["date"] = df["open_dt"].dt.normalize()
    df["year"]      = df["open_dt"].dt.year.astype("Int32")
    df["month"]     = df["open_dt"].dt.month.astype("Int32")
    df["day"]       = df["open_dt"].dt.day.astype("Int32")
    df["dayofweek"] = df["open_dt"].dt.dayofweek.astype("Int32")
    df["hour"]      = df["open_dt"].dt.hour.astype("Int32")
    df["week"]      = df["open_dt"].dt.isocalendar().week.astype("UInt32")

    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Clean neighborhood column
    df["neighborhood"] = df["neighborhood"].str.strip()

    # Drop rows with no usable neighborhood
    df = df[df["neighborhood"].notna()]
    df = df[df["neighborhood"] != ""]
    df = df[~df["neighborhood"].isin(DROP_NEIGHBORHOODS)] # Drop city-wide catch-alls
    df["neighborhood"] = df["neighborhood"].replace(NEIGHBORHOOD_MERGE_MAP) # Merge fragmented labels into canonical names
    df["neighborhood_clean"] = df["neighborhood"]

    # Bucket rare request types into "Other"
    type_counts = df["type"].value_counts()
    rare_types  = type_counts[type_counts < MIN_TYPE_COUNT].index
    df["type_clean"] = df["type"].where(~df["type"].isin(rare_types), other="Other")

    return df

# Time series aggreagtions (daily, weekly, by type, by neighborhood)
def _full_date_range(df: pd.DataFrame, date_col: str = "date") -> pd.DatetimeIndex:
    return pd.date_range(df[date_col].min(), df[date_col].max(), freq="D")


def make_daily_total(df: pd.DataFrame) -> pd.DataFrame:
    """(date, count) — citywide daily totals, no gaps."""
    daily = (
        df.groupby("date")
        .size()
        .reindex(_full_date_range(df), fill_value=0)
        .rename("count")
        .reset_index()
        .rename(columns={"index": "date"})
    )
    return daily


def make_daily_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """Wide pivot: date by request_type"""
    long = df.groupby(["date", "type_clean"]).size().reset_index(name="count")
    wide = (
        long.pivot(index="date", columns="type_clean", values="count")
        .fillna(0)
        .astype(int)
        .reindex(_full_date_range(df), fill_value=0)
    )
    wide.index.name = "date"
    out = wide.reset_index()
    out.columns.name = None  # clear axis name so parquet columns are clean
    return out


def make_daily_by_neighborhood(df: pd.DataFrame) -> pd.DataFrame:
    """Wide pivot: date x neighborhood_clean
    Column names match the original notebook (neighborhood_clean as pivot axis).
    """
    long = df.groupby(["date", "neighborhood_clean"]).size().reset_index(name="count")
    wide = (
        long.pivot(index="date", columns="neighborhood_clean", values="count")
        .fillna(0)
        .astype(int)
        .reindex(_full_date_range(df), fill_value=0)
    )
    wide.index.name = "date"
    out = wide.reset_index()
    out.columns.name = None  # clear axis name so parquet columns are clean
    return out


def make_weekly_total(df: pd.DataFrame) -> pd.DataFrame:
    """(week_start, count) for weekly totals"""
    df = df.copy()
    df["week_start"] = df["open_dt"] - pd.to_timedelta(df["open_dt"].dt.dayofweek, unit="d")
    df["week_start"] = df["week_start"].dt.normalize()
    weekly = (
        df.groupby("week_start")
        .size()
        .rename("count")
        .reset_index()
        .rename(columns={"index": "week_start"})
        .sort_values("week_start")
    )
    return weekly


def make_weekly_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """Wide pivot: week_start by request_type."""
    df = df.copy()
    df["week_start"] = df["open_dt"] - pd.to_timedelta(df["open_dt"].dt.dayofweek, unit="d")
    df["week_start"] = df["week_start"].dt.normalize()
    long = df.groupby(["week_start", "type_clean"]).size().reset_index(name="count")
    wide = (
        long.pivot(index="week_start", columns="type_clean", values="count")
        .fillna(0)
        .astype(int)
    )
    wide.index.name = "week_start"
    out = wide.reset_index()
    out.columns.name = None
    return out


def make_daily_features(daily_total: pd.DataFrame) -> pd.DataFrame:
    """
    Add calendar + lag + rolling features to daily_total
    """
    feat = daily_total.copy()
    feat = feat.sort_values("date").reset_index(drop=True)

    feat["dayofweek"]    = feat["date"].dt.dayofweek
    feat["month"]        = feat["date"].dt.month
    feat["year"]         = feat["date"].dt.year
    feat["day_of_year"]  = feat["date"].dt.dayofyear
    feat["quarter"]      = feat["date"].dt.quarter
    feat["is_weekend"]   = feat["dayofweek"].isin([5, 6]).astype(int)

    for lag in [1, 7, 14, 30]:
        feat[f"count_lag_{lag}"] = feat["count"].shift(lag)

    for window in [7, 14, 30]:
        feat[f"count_rolling_mean_{window}"] = (
            feat["count"].rolling(window=window, min_periods=1).mean()
        )

    feat["days_since_start"] = (feat["date"] - feat["date"].min()).dt.days

    return feat


def make_cleaned(df: pd.DataFrame) -> pd.DataFrame:
    """
    Row-level cleaned file matching the original notebook's 311_cleaned.parquet.
    Exact columns: open_dt, date, type_clean, neighborhood_clean,
                   subject, reason, latitude, longitude,
                   year, month, dayofweek, hour
    """
    return df[[
        "open_dt", "date", "type_clean", "neighborhood_clean",
        "subject", "reason", "latitude", "longitude",
        "year", "month", "dayofweek", "hour",
    ]].copy()


# Bayesian aggregation

def make_bayesian_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Long format (date, neighborhood, count) for Bayesian regression.
    This is the format the Bayesian model needs — one row per
    neighborhood per day, with the count of requests as the target.
    """
    long = (
        df.groupby(["date", "neighborhood"])
        .size()
        .rename("count")
        .reset_index()
        .sort_values(["neighborhood", "date"])
        .reset_index(drop=True)
    )
    long["count"] = long["count"].astype("int64")
    return long


# Export

def export(df: pd.DataFrame, name: str, out_dir: Path) -> None:
    path = out_dir / name
    df.to_parquet(path, index=False)
    print(f"  Wrote {len(df):>8,} rows  →  {path.name}")


def run(raw_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # -- shared cleaning -----------------------------------------------------
    df = load_and_clean(raw_path)

    # -- time series outputs -------------------------------------------------
    print("\nBuilding time series outputs ...")

    daily_total = make_daily_total(df)
    export(daily_total, "daily_total.parquet", out_dir)

    export(make_daily_by_type(df),         "daily_by_type.parquet",         out_dir)
    export(make_daily_by_neighborhood(df), "daily_by_neighborhood.parquet", out_dir)
    export(make_weekly_total(df),          "weekly_total.parquet",          out_dir)
    export(make_weekly_by_type(df),        "weekly_by_type.parquet",        out_dir)
    export(make_daily_features(daily_total), "daily_features.parquet",      out_dir)
    export(make_cleaned(df),                 "311_cleaned.parquet",          out_dir)

    # -- bayesian output -----------------------------------------------------
    export(make_bayesian_long(df), "bayesian_preprocessed.parquet", out_dir)

    print("\nPreprocessing done")


if __name__ == "__main__":
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Raw parquet not found: {RAW_DATA_PATH}\n"
            f"Set RAW_DATA_PATH env var, e.g.:\n"
            f"  RAW_DATA_PATH=/path/to/311_2019_2024.parquet python preprocessing.py"
        )
    run(RAW_DATA_PATH, PROCESSED_DIR)