## SQL queries as a homoework

### Q3: Count of records in fct_monthly_zone_revenue?
```sql
select count(*) 
from `just-aura-484716-a1.dbt_anna.fct_monthly_zone_revenue`
```


### Question 4. Q4: Zone with highest revenue for Green taxis in 2020? 
```sql
select
  pickup_zone,
  sum(revenue_monthly_total_amount) as revenue
from `just-aura-484716-a1.dbt_anna.fct_monthly_zone_revenue`
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by revenue desc
limit 10;
```

### Question 5. Q5: Total trips for Green taxis in October 2019? 
```sql
select
  sum(total_monthly_trips) as total_trips
from `just-aura-484716-a1.dbt_anna.fct_monthly_zone_revenue`
where service_type = 'Green'
  and revenue_month = '2019-10-01'
```

### Question 6. Q6: Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?

```sql
select count(*)
from `just-aura-484716-a1.dbt_anna.stg_fhv_tripdata`;
```

OR

```sql
select count(*) as record_count
from `just-aura-484716-a1.taxi_data.fhv_tripdata`
where dispatching_base_num is not null;
```

![image](https://github.com/AnnaPrus/data-engineering-zoomcamp/blob/59c8f851abc735c96a264a99d9480f43e5dd9edd/images/bigquery_folders.png)
