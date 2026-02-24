# Start and reset docker compose
```bash
docker compose down -v
docker compose up -d
```

The queries for solving the quiz: 
```sql
SELECT count(*) FROM `my_project_id.zoomcamp.green_tripdata` WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) >= TIMESTAMP("2020-01-01") and
TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) < TIMESTAMP("2021-01-01") 
```

```sql
SELECT count(*) FROM `just-aura-484716-a1.zoomcamp.yellow_tripdata` WHERE TIMESTAMP_TRUNC(tpep_pickup_datetime, DAY) >= TIMESTAMP("2021-03-01") and
TIMESTAMP_TRUNC(tpep_pickup_datetime, DAY) < TIMESTAMP("2021-04-01") 
```
