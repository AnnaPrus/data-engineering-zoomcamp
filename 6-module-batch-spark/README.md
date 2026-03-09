# Spark Homework – Data Engineering Zoomcamp (Module 6)

This project demonstrates basic batch processing with **PySpark** using the NYC Yellow Taxi dataset.
We run Spark locally in GitHub Codespaces and perform several queries on the dataset.

---

## 1. Environment Setup

This project uses **Python + PySpark** managed with **uv**.

### Install uv

```bash
pip install uv
```

### Create and activate environment

```bash
uv venv
source .venv/bin/activate
```

### Install PySpark

We use Spark **3.5.0**, which avoids compatibility issues with newer Java versions.

```bash
uv add pyspark==3.5.0
```

Verify installation:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

Expected output:

```
3.5.0
```

---

## 2. Download the Dataset

Download the November 2025 Yellow Taxi dataset:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

Download the taxi zone lookup table:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

---

## 3. Create a Spark Session

We start a local Spark session using all available CPU cores.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("spark-homework")
    .getOrCreate()
)

print("Spark version:", spark.version)
```

---

## 4. Load the Dataset

Read the Parquet file into a Spark DataFrame:

```python
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.printSchema()
```

---

## 5. Repartition and Write Parquet (Question 2)

We repartition the dataset into **4 partitions** and write it back to disk.

```python
df.repartition(4).write.mode("overwrite").parquet("yellow_repartitioned")
```

Each partition becomes one Parquet file.
The average file size is approximately **25 MB**.

---

## 6. Count Trips on November 15 (Question 3)

```python
from pyspark.sql.functions import to_date, col

trips = df.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
).count()

print(trips)
```

Result:

```
162604
```

---

## 7. Longest Trip Duration (Question 4)

Compute trip duration in hours:

```python
from pyspark.sql.functions import unix_timestamp

df_with_duration = df.withColumn(
    "trip_hours",
    (unix_timestamp("tpep_dropoff_datetime") -
     unix_timestamp("tpep_pickup_datetime")) / 3600
)

df_with_duration.agg({"trip_hours": "max"}).show()
```

Result:

```
90.6 hours
```

---

## 8. Spark UI (Question 5)

Spark provides a monitoring dashboard available at:

```
http://localhost:4040
```

The UI shows jobs, stages, and task execution details.

---

## 9. Least Frequent Pickup Zone (Question 6)

Load the taxi zone lookup table:

```python
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")
```

Run a SQL query:

```python
df.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

spark.sql("""
SELECT z.Zone, COUNT(*) as pickups
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY pickups ASC
LIMIT 1
""").show()
```

Result:

```
Rikers Island
```

---

## Run the Script

Execute the Spark script:

```bash
uv run python spark_session.py
```

---

## References

* Data Engineering Zoomcamp
* Apache Spark Documentation
