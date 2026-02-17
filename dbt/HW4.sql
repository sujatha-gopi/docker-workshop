SELECT COUNT(*) 
FROM `mythic-altar-485103-v8.dbt_sgopi_prod.fct_monthly_zone_revenue`;


SELECT pickup_zone, SUM(revenue_monthly_total_amount) AS total_revenue
FROM `mythic-altar-485103-v8.dbt_sgopi_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;



SELECT SUM(total_monthly_trips)
FROM `mythic-altar-485103-v8.dbt_sgopi_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND revenue_month = '2019-10-01';


SELECT
    SUM(total_monthly_trips)
FROM `mythic-altar-485103-v8.dbt_sgopi_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND revenue_month = '2019-10-01';


SELECT count(1) 
FROM `mythic-altar-485103-v8.dbt_sgopi_prod.stg_fhv_tripdata`;