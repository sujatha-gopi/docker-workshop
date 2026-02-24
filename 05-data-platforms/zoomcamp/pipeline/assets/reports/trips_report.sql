/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  #strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: trip_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: trip_date
    type: date
    primary_key: true
  - name: taxi_type
    type: string
    primary_key: true
  - name: payment_type
    type: string
    primary_key: true

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

-- trips_report.sql
-- Summary report: daily and overall trip metrics
-- Replace `taxi_trips` with your actual table/view name if different.

WITH base AS (
  SELECT
    pickup_datetime::date AS trip_date,
    EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0 AS duration_min,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount
  FROM staging.trips
  WHERE pickup_datetime >= CAST('{{ start_date }}' AS timestamp)
    AND pickup_datetime <= CAST('{{ end_date }}' AS timestamp)
),

daily AS (
  SELECT
    trip_date,
    COUNT(*) AS trips,
    ROUND(AVG(duration_min), 2) AS avg_duration_min,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_min), 2) AS median_duration_min,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(SUM(fare_amount + tip_amount + tolls_amount), 2) AS daily_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
  FROM base
  GROUP BY trip_date
  ORDER BY trip_date
),

overall AS (
  SELECT
    'overall' AS period,
    COUNT(*) AS trips,
    ROUND(AVG(duration_min), 2) AS avg_duration_min,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_min), 2) AS median_duration_min,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(SUM(fare_amount + tip_amount + tolls_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
  FROM base
)

SELECT * FROM daily
UNION ALL
SELECT
  NULL AS trip_date,
  trips,
  avg_duration_min,
  median_duration_min,
  avg_distance_miles,
  total_revenue,
  avg_total_amount
FROM overall;