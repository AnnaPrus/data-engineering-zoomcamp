/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: |
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

WITH source_trips AS (
    SELECT
        -- Normalize pickup timestamp across taxi types
        t.tpep_pickup_datetime AS pickup_datetime,
        t.taxi_type,
        t.vendor_id,
        t.passenger_count,
        t.trip_distance,
        t.payment_type,
        t.total_amount,
        t.extracted_at
    FROM ingestion.trips AS t
    WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
      AND t.tpep_pickup_datetime < '{{ end_datetime }}'
),
deduped AS (
    SELECT
        *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    s.pickup_datetime,
                    s.vendor_id,
                    s.taxi_type,
                    s.trip_distance,
                    s.total_amount
                ORDER BY s.extracted_at DESC
            ) AS rn
        FROM source_trips AS s
        WHERE s.pickup_datetime IS NOT NULL
          AND s.total_amount IS NOT NULL
          AND s.total_amount >= 0
    )
    WHERE rn = 1
)
SELECT
    d.pickup_datetime,
    d.taxi_type,
    d.vendor_id,
    d.passenger_count,
    d.trip_distance,
    d.payment_type,
    pl.payment_type_name,
    d.total_amount,
    d.extracted_at
FROM deduped AS d
LEFT JOIN ingestion.payment_lookup AS pl
    ON d.payment_type = pl.payment_type_id;

