/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

checks:
  - name: row_count_positive
    description: Ensures the report has at least one row
    query: |
      SELECT COUNT(*) > 0 FROM reports.trips_report
    value: 1
  - name: total_amount_non_negative
    description: Ensures aggregated total_amount is never negative
    query: |
      SELECT MIN(total_amount) >= 0 FROM reports.trips_report
    value: 1

@bruin */

WITH source AS (
    SELECT
        pickup_datetime,
        taxi_type,
        payment_type_name,
        total_amount
    FROM staging.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
),
aggregated AS (
    SELECT
        DATE_TRUNC('day', pickup_datetime) AS pickup_date,
        taxi_type,
        payment_type_name,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_amount,
        AVG(total_amount) AS avg_total_amount
    FROM source
    GROUP BY
        pickup_date,
        taxi_type,
        payment_type_name
)
SELECT
    pickup_date,
    taxi_type,
    payment_type_name,
    trip_count,
    total_amount,
    avg_total_amount
FROM aggregated;

