"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"

@bruin"""

import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
RAW_OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("trips_ingest")


# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
  start = os.getenv("BRUIN_START_DATE")
  end = os.getenv("BRUIN_END_DATE")
  if not start or not end:
      logger.error("Environment variables BRUIN_START_DATE and BRUIN_END_DATE are required.")
      raise SystemExit(2)

  start_date = parse_date(start)
  end_date = parse_date(end)
  if start_date >= end_date:
      logger.error("BRUIN_START_DATE must be before BRUIN_END_DATE")
      raise SystemExit(2)

  taxi_types = parse_taxi_types()
  logger.info(f"Ingesting taxi types: {taxi_types}")
  logger.info(f"Date range: {start_date} -> {end_date} (end exclusive)")

  ensure_out_dir()

  all_dfs = []
  for month_dt in month_iter(start_date, end_date):
      for taxi in taxi_types:
          filename = build_filename(taxi, month_dt)
          url = BASE_URL + filename
          logger.info(f"Downloading {url}")
          try:
              b = download_bytes(url)
          except requests.HTTPError as e:
              logger.warning(f"Failed to download {url}: {e}. Skipping.")
              continue
          except Exception as e:
              logger.warning(f"Error downloading {url}: {e}. Skipping.")
              continue

          try:
              df = read_parquet_from_bytes(b)
              all_dfs.append(df)
          except Exception as e:
              logger.warning(f"Failed to read parquet for {url}: {e}. Skipping.")
              continue

          out_file = RAW_OUT_DIR / f"{taxi}_{month_dt.year}-{month_dt.month:02d}.parquet"
          logger.info(f"Saving raw parquet to {out_file}")
          save_parquet(df, out_file)
  
  logger.info("Ingestion complete.")
  if not all_dfs:
    logger.warning("No data downloaded for given interval.")
    return pd.DataFrame()  # return empty dataframe

  final_df = pd.concat(all_dfs, ignore_index=True)
  return final_df 
  # return final_dataframe

def parse_taxi_types() -> List[str]:
    # Check BRUIN_VARS JSON first
    bruin_vars = os.getenv("BRUIN_VARS")
    if bruin_vars:
        try:
            parsed = json.loads(bruin_vars)
            if isinstance(parsed, dict) and "taxi_types" in parsed:
                return list(parsed["taxi_types"])
        except Exception:
            logger.debug("BRUIN_VARS exists but is not JSON or missing taxi_types")

    # Fallback to TAXI_TYPES env var (comma-separated) or default
    env_types = os.getenv("TAXI_TYPES")
    if env_types:
        return [t.strip() for t in env_types.split(",") if t.strip()]

    return ["yellow"]


def parse_date(date_str: str) -> datetime.date:
    return datetime.fromisoformat(date_str).date()


def month_iter(start_date, end_date) -> Iterable[datetime]:
    # Yields (year, month) for months from start_date up to but not including end_date
    cur = datetime(start_date.year, start_date.month, 1)
    end = datetime(end_date.year, end_date.month, 1)
    while cur < end:
        yield cur
        cur += relativedelta(months=1)


def build_filename(taxi_type: str, dt: datetime) -> str:
    return f"{taxi_type}_tripdata_{dt.year}-{dt.month:02d}.parquet"


def download_bytes(url: str, timeout: int = 60) -> bytes:
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    buffer = io.BytesIO()
    for chunk in r.iter_content(chunk_size=1024 * 1024):
        if chunk:
            buffer.write(chunk)
    return buffer.getvalue()


def read_parquet_from_bytes(b: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(b), engine="pyarrow")


def ensure_out_dir() -> None:
    RAW_OUT_DIR.mkdir(parents=True, exist_ok=True)


def save_parquet(df: pd.DataFrame, out_path: Path) -> None:
    df.to_parquet(out_path, index=False)

