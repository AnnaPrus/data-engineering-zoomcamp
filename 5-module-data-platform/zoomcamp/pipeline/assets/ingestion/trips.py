"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: "Taxi service type (e.g., yellow, green)."
  - name: vendorid
    type: integer
    description: "TLC vendor code from the raw trip data."
  - name: tpep_pickup_datetime
    type: timestamp
    description: "Pickup timestamp for yellow taxi trips (raw column)."
  - name: lpep_pickup_datetime
    type: timestamp
    description: "Pickup timestamp for green taxi trips (raw column)."
  - name: passenger_count
    type: integer
    description: "Number of passengers reported by the driver."
  - name: trip_distance
    type: float
    description: "Trip distance in miles from the meter."
  - name: payment_type
    type: integer
    description: "Numeric payment type code from TLC schema."
  - name: total_amount
    type: float
    description: "Total amount charged for the trip (USD)."
  - name: extracted_at
    type: timestamp
    description: "UTC timestamp when the record was ingested."

@bruin"""

import io
import json
import os
from datetime import date, datetime

from dateutil.relativedelta import relativedelta
import pandas as pd
import requests


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _parse_date(value: str, var_name: str) -> date:
    if not value:
        raise ValueError(f"{var_name} environment variable is required for this asset.")
    return datetime.strptime(value, "%Y-%m-%d").date()


def _month_range(start: date, end: date):
    """Yield the first day of each month between start and end (inclusive)."""
    current = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while current <= last:
        yield current
        current += relativedelta(months=1)


def _get_taxi_types() -> list[str]:
    """Read taxi_types pipeline variable from BRUIN_VARS (defaults to ['yellow'])."""
    raw = os.getenv("BRUIN_VARS") or "{}"
    try:
        vars_dict = json.loads(raw)
    except json.JSONDecodeError:
        vars_dict = {}

    taxi_types = vars_dict.get("taxi_types") or ["yellow"]
    if isinstance(taxi_types, str):
        # Allow simple comma-separated string, but canonical form is a JSON array.
        taxi_types = [t.strip() for t in taxi_types.split(",") if t.strip()]
    return taxi_types


def materialize() -> pd.DataFrame:
    """
    Ingest raw NYC taxi trips for the current Bruin date window and configured taxi_types.

    - Uses BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD) to determine which monthly files to fetch.
    - Reads the taxi_types pipeline variable from BRUIN_VARS (defaults to ["yellow"]).
    - Fetches Parquet files from the TLC CloudFront endpoint.
    - Concatenates all months/types into a single DataFrame.
    - Adds taxi_type and extracted_at columns.
    - Returns a DataFrame for append-only loading into the ingestion.trips table.
    """
    start_date = _parse_date(os.getenv("BRUIN_START_DATE"), "BRUIN_START_DATE")
    end_date = _parse_date(os.getenv("BRUIN_END_DATE"), "BRUIN_END_DATE")
    taxi_types = _get_taxi_types()

    frames: list[pd.DataFrame] = []
    extracted_at = datetime.utcnow()

    for taxi_type in taxi_types:
        for month_start in _month_range(start_date, end_date):
            month_str = month_start.strftime("%Y-%m")
            filename = f"{taxi_type}_tripdata_{month_str}.parquet"
            url = f"{BASE_URL}/{filename}"

            response = requests.get(url, stream=True)
            if response.status_code == 404:
                # No data available for this taxi_type/month; skip quietly.
                continue
            response.raise_for_status()

            table_bytes = io.BytesIO(response.content)
            df = pd.read_parquet(table_bytes)

            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            frames.append(df)

    if not frames:
        # No data for this window; return an empty DataFrame with expected metadata columns.
        return pd.DataFrame(
            columns=[
                "taxi_type",
                "vendorid",
                "tpep_pickup_datetime",
                "lpep_pickup_datetime",
                "passenger_count",
                "trip_distance",
                "payment_type",
                "total_amount",
                "extracted_at",
            ]
        )

    return pd.concat(frames, ignore_index=True, sort=False)

