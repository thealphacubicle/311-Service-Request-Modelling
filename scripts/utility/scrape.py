import asyncio
import httpx
import pandas as pd

SQL_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"

RESOURCE_IDS = {
    2019: "ea2e4696-4a2d-429c-9807-d02eb92e0222",
    2020: "6ff6a6fd-3141-4440-a880-6f60a37fe789",
    2021: "f53ebccd-bc61-49f9-83db-625f209c95f5",
    2022: "81a7b022-f8fc-4da5-80e4-b160058ca207",
    2023: "e6013a93-1321-4f2a-bf91-8d8a02f1e62f",
    2024: "dff4d804-5031-443a-8409-8344efd0e5c8",
}

COLUMNS = (
    "open_dt, case_title, subject, reason, type, neighborhood, latitude, longitude"
)

sem = asyncio.Semaphore(3)  # max 3 concurrent requests to avoid rate limiting


async def fetch_page(client, resource_id, year, offset, limit=10000):
    async with sem:
        query = f"""
            SELECT {COLUMNS}
            FROM "{resource_id}"
            ORDER BY open_dt ASC
            LIMIT {limit} OFFSET {offset}
        """
        resp = await client.get(SQL_URL, params={"sql": query})
        data = resp.json()
        if not data["success"]:
            raise Exception(
                f"Query failed for {year} at offset {offset}: {data['error']}"
            )
        return data["result"]["records"]


async def fetch_year(client, year, resource_id, limit=10000):
    records = []
    offset = 0
    while True:
        batch = await fetch_page(client, resource_id, year, offset, limit)
        if not batch:
            break
        records.extend(batch)
        offset += limit
        print(f"{year}: fetched {len(records)} records...")
    df = pd.DataFrame(records)
    df["year"] = year
    return df


async def fetch_all_years():
    async with httpx.AsyncClient(timeout=60.0) as client:
        tasks = [fetch_year(client, year, rid) for year, rid in RESOURCE_IDS.items()]
        dfs = await asyncio.gather(*tasks)

    combined = pd.concat(dfs, ignore_index=True)
    combined.to_parquet("data/raw/311_2019_2024.parquet", index=False)
    print(f"\nDone. Total records: {len(combined)}")
    return combined


if __name__ == "__main__":
    import os

    parquet_path = "data/raw/311_2019_2024.parquet"
    if os.path.exists(parquet_path):
        print(f"\nFile '{parquet_path}' already exists. Skipping download.\n")
    else:
        print(f"\nDownloading data to '{parquet_path}'...\n")
        asyncio.run(fetch_all_years())
        print(f"\nDone. Total records: {len(combined)}")
