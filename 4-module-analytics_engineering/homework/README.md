## SQL queries as a homoework


´´´sql
select count(*) 
from `just-aura-484716-a1.dbt_anna.fct_monthly_zone_revenue`
´´´


select
  pickup_zone,
  sum(revenue_monthly_total_amount) as revenue
from `just-aura-484716-a1.dbt_anna.fct_monthly_zone_revenue`
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by revenue desc
limit 10;



select
  sum(total_monthly_trips) as total_trips
from `just-aura-484716-a1.dbt_anna.fct_monthly_zone_revenue`
where service_type = 'Green'
  and revenue_month = '2019-10-01'
