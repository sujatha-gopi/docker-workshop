/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1
@bruin */

WITH raw AS (
  SELECT 
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    *
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= CAST('{{ start_date }}' AS timestamp)
    AND tpep_pickup_datetime <= CAST('{{ end_date }}' AS timestamp)
),

enriched AS (
  SELECT
    r.*,
    p.payment_type_id,
    p.payment_type_name
  FROM raw r
  LEFT JOIN ingestion.payment_lookup p
    ON r.payment_type = p.payment_type_id
),

deduped AS (
  SELECT
    -- keep one row per composite trip signature
    *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          pickup_datetime,
          dropoff_datetime,
          pu_location_id,
          do_location_id,
          passenger_count,
          fare_amount
        ORDER BY pickup_datetime
      ) AS rn
    FROM enriched e
  ) t
  WHERE rn = 1
)

SELECT *
FROM deduped;