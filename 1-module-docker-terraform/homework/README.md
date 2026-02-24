# Data Engineering Zoomcamp: Homework for Module 1

This repository contains exercises and homework for the **Data Engineering Zoomcamp**.
It focuses on Python environments, Docker, and ingesting Taxi Trips data into Postgres.

## Project Structure

```text
1-module-docker-terraform/
└── homework/
    ├── Dockerfile
    ├── docker-compose.yaml
    ├── ingest_data_exploration.py
    ├── ingest_data_exploration.ipynb
    ├── main.py
    ├── pyproject.toml
    ├── uv.lock
    ├── .python-version
    ├── .gitignore
```

---
## Solution for the SQL query part:

```sql
SELECT COUNT(*)
FROM green_trip_data_2025
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

```sql
SELECT
    DATE(lpep_pickup_datetime) as pick_up_time,
    SUM(trip_distance) AS total_trip_distance
FROM green_trip_data_2025
WHERE trip_distance < 100
GROUP BY pick_up_time
ORDER BY total_trip_distance DESC
LIMIT 1;
```


```sql
SELECT "PULocationID" as pick_up_id, 
 DATE(lpep_pickup_datetime) as pick_up_time, 
 SUM(trip_distance) AS total_trip_distance
 FROM green_trip_data_2025
 GROUP BY pick_up_time, pick_up_id
```


```sql
SELECT
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_revenue
FROM green_trip_data_2025 t
JOIN taxi_zone_lookup z
  ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_revenue DESC
LIMIT 1;
```
---
## Getting Started & Usage

This project uses **uv** for Python dependency management.

## We use uv - a modern, fast Python package and project manager written in Rust.

```bash
pip install uv
```

## initialize Python project with uv

```bash
uv init --python=3.13
uv run which python
```

## Add a dependency

```bash
uv add pandas sqlalchemy psycopg2-binary click
```

## Run a Python script

```bash
uv run python ingest_data_exploration.py
```

## Run Jupyter Notebook

```bash
uv uv run jupyter notebook
```
## Start two containers in the same docker network to ingest the data into Postgre:

```bash
#Create a network
docker network create pg-network


# Run PostgreSQL on the network
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="trip_data" \
  -v trip_data_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

# Build a docker image
docker build -t zoomcamp-ingest .

# In another terminal, run Image on the same network
docker run -it \
  --network=pg-network \
  zoomcamp-ingest \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=trip_data \
    --table-trips=green_trip_data_2025 \
     --table-zones=taxi_zone_lookup \
```