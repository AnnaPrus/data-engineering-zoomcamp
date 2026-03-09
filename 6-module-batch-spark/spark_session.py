from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, unix_timestamp

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("spark-test")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    .getOrCreate()
)

print("Spark version:", spark.version)

# Read taxi dataset
df = spark.read.parquet("./yellow_tripdata_2025-11.parquet")

# -------------------------
# Question 2
# -------------------------
df.repartition(4).write.mode("overwrite").parquet("yellow_repartitioned")

# -------------------------
# Question 3
# -------------------------
trips = df.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
).count()

print("Trips on Nov 15:", trips)

# -------------------------
# Question 4 (Longest trip)
# -------------------------
df_with_duration = df.withColumn(
    "trip_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)

longest_trip = df_with_duration.agg({"trip_hours": "max"}).collect()[0][0]

print("Longest trip hours:", longest_trip)

# -------------------------
# Question 6
# -------------------------
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

df.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

least_zone = spark.sql("""
SELECT z.Zone, COUNT(*) as pickups
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY pickups ASC
LIMIT 1
""")

least_zone.show()

input("Press Enter to stop Spark")

spark.stop()