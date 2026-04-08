"""
preprocessing.py
----------------
Unified preprocessing for Boston 311 Service Request data.

Produces outputs for two downstream models:

  Time Series outputs  (citywide + neighborhood aggregates)
  ├── daily_total.parquet            – (date, count)
  ├── daily_by_type.parquet          – wide pivot: date × request_type
  ├── daily_by_neighborhood.parquet  – wide pivot: date × neighborhood
  ├── weekly_total.parquet           – (week_start, count)
  ├── weekly_by_type.parquet         – wide pivot: week_start × request_type
  └── daily_features.parquet         – daily_total + lag/rolling features

  Bayesian output
  └── bayesian_preprocessed.parquet  – long format: (date, neighborhood, count)

Usage
-----
    python preprocessing.py                         # uses default paths
    RAW_DATA_PATH=... PROCESSED_DIR=... python preprocessing.py
"""

import os
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = REPO_ROOT / "data" / "raw" / "311_2019_2024.parquet"
DEFAULT_PROCESSED_DIR = REPO_ROOT / "data" / "processed"

RAW_DATA_PATH = Path(os.environ.get("RAW_DATA_PATH", DEFAULT_RAW))
PROCESSED_DIR = Path(os.environ.get("PROCESSED_DIR", DEFAULT_PROCESSED_DIR))


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Request types with fewer than this many records get bucketed into "Other"
MIN_TYPE_COUNT = 1_000

# Neighborhoods to drop entirely (city-wide catch-alls with no geographic meaning)
DROP_NEIGHBORHOODS = {"Boston", "Chestnut Hill"}

# Merge fragmented neighborhood labels into a single canonical name
NEIGHBORHOOD_MERGE_MAP = {
    "Allston": "Allston / Brighton",
    "Brighton": "Allston / Brighton",
    "South Boston / South Boston Waterfront": "South Boston",
    "Greater Mattapan": "Mattapan",
}


# ---------------------------------------------------------------------------
# Step 1: Load & core cleaning  (shared by both models)
# ---------------------------------------------------------------------------

def load_and_clean(path: Path) -> pd.DataFrame:
    """
    Load the raw parquet and apply all cleaning that both models need:
      - parse open_dt
      - extract date/time features
      - drop/fill missing neighborhoods
      - merge duplicate neighborhood labels
      - drop city-wide catch-alls
      - bucket rare request types into "Other"
    """
    print(f"Loading raw data from {path} ...")
    df = pd.read_parquet(path)
    print(f"  Loaded {len(df):,} records")

    # --- datetime parsing ---------------------------------------------------
    df["open_dt"] = pd.to_datetime(df["open_dt"], errors="coerce")
    # Use dt.normalize() so date is still a proper Timestamp (not a Python date
    # object), which avoids the dtype=object bug in the original notebook.
    df["date"] = df["open_dt"].dt.normalize()

    df["year"]      = df["open_dt"].dt.year.astype("Int32")
    df["month"]     = df["open_dt"].dt.month.astype("Int32")
    df["day"]       = df["open_dt"].dt.day.astype("Int32")
    df["dayofweek"] = df["open_dt"].dt.dayofweek.astype("Int32")
    df["hour"]      = df["open_dt"].dt.hour.astype("Int32")
    df["week"]      = df["open_dt"].dt.isocalendar().week.astype("UInt32")

    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # --- neighborhood cleaning ----------------------------------------------
    df["neighborhood"] = df["neighborhood"].str.strip()

    # Drop rows with no usable neighborhood (null, empty string, or catch-alls)
    df = df[df["neighborhood"].notna()]
    df = df[df["neighborhood"] != ""]
    df = df[~df["neighborhood"].isin(DROP_NEIGHBORHOODS)]

    # Merge fragmented labels into canonical names
    df["neighborhood"] = df["neighborhood"].replace(NEIGHBORHOOD_MERGE_MAP)

    # Keep neighborhood_clean as an alias so downstream files that reference
    # that column name (daily_by_neighborhood wide pivot, 311_cleaned) are
    # byte-for-byte compatible with what the original notebook produced.
    df["neighborhood_clean"] = df["neighborhood"]

    # --- request type cleaning ----------------------------------------------
    type_counts = df["type"].value_counts()
    rare_types  = type_counts[type_counts < MIN_TYPE_COUNT].index
    df["type_clean"] = df["type"].where(~df["type"].isin(rare_types), other="Other")

    print(f"  After cleaning: {len(df):,} records")
    print(f"  Neighborhoods : {df['neighborhood'].nunique()}")
    print(f"  Request types : {df['type_clean'].nunique()} (after bucketing rare)")
    return df


# ---------------------------------------------------------------------------
# Step 2: Time series aggregations
# ---------------------------------------------------------------------------

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
    """Wide pivot: date × request_type, no gaps."""
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
    """Wide pivot: date x neighborhood_clean, no gaps.
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
    """(week_start, count) — citywide weekly totals."""
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
    """Wide pivot: week_start × request_type."""
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
    Enrich daily_total with calendar + lag + rolling features.
    These are the input features for ML-style time series models.
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


# ---------------------------------------------------------------------------
# Step 3: Bayesian aggregation
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Step 4: Export
# ---------------------------------------------------------------------------

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
    print("\nBuilding Bayesian output ...")
    export(make_bayesian_long(df), "bayesian_preprocessed.parquet", out_dir)

    print("\nDone. All outputs written to:", out_dir)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Raw parquet not found: {RAW_DATA_PATH}\n"
            f"Set RAW_DATA_PATH env var, e.g.:\n"
            f"  RAW_DATA_PATH=/path/to/311_2019_2024.parquet python preprocessing.py"
        )
    run(RAW_DATA_PATH, PROCESSED_DIR)