### Getting Started & Usage

This project uses **uv** for Python dependency management.

### We use uv - a modern, fast Python package and project manager written in Rust.

```bash
pip install uv
```

### initialize Python project with uv

```bash
uv init --python=3.13
uv run which python
```

### Add a dependency

```bash
uv add pandas sqlalchemy psycopg2-binary click
```

### Run a Python script

```bash
uv run python ingest_data_exploration.py
```

### External Table: BigQuery reads data directly from files in Google Cloud Storage (GCS)
```sql
CREATE OR REPLACE EXTERNAL TABLE [my_project.taxi_data].yellow_taxi_external
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025_anna/yellow_tripdata_*.parquet']
);
```

### Regular (Native) Table: BigQuery copies the data into its own storage

```sql
CREATE OR REPLACE TABLE taxi_data.yellow_taxi_native AS
SELECT *
FROM taxi_data.yellow_taxi_external;
```

### Test the tables
```sql
SELECT COUNT(*) FROM taxi_data.yellow_taxi_external;
SELECT COUNT(*) FROM taxi_data.yellow_taxi_native;
```

## SOLUTIONS
### Question 1: What is count of records for the 2024 Yellow Taxi Data?

```sql
select count(*)
from taxi_data.yellow_taxi_native
where tpep_pickup_datetime >= '2024-01-01' 
and tpep_pickup_datetime < '2025-01-01';
```

### Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_locations
FROM taxi_data.yellow_taxi_native;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_locations
FROM taxi_data.yellow_taxi_external;

```

### Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```sql
SELECT PULocationID
FROM taxi_data.yellow_taxi_native;

SELECT PULocationID, DOLocationID
FROM taxi_data.yellow_taxi_native;
```

### Question 4: How many records have a fare_amount of 0?

```sql
select count(*)
from taxi_data.yellow_taxi_external
where fare_amount = 0;
```
### Question 5: create a partitioned table from your Yellow Taxi table.

```sql
CREATE OR REPLACE TABLE taxi_data.yellow_taxi_partitioned
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM taxi_data.yellow_taxi_native;
```



### Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

```sql
select distinct VendorID
from taxi_data.yellow_taxi_native
where tpep_dropoff_datetime > '2024-03-01'
and tpep_dropoff_datetime <= '2024-03-15'
order by VendorID;

select distinct VendorID
from taxi_data.yellow_taxi_partitioned
where tpep_dropoff_datetime > '2024-03-01'
and tpep_dropoff_datetime <= '2024-03-15'
order by VendorID;
```
