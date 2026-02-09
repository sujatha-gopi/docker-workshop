#HW 3:

CREATE OR REPLACE EXTERNAL TABLE `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mythic-altar-485103-v8-bucket/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*) 
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_external`;

CREATE OR REPLACE TABLE `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`
AS
SELECT *
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_external`;

SELECT
  _FILE_NAME AS file,
  COUNT(*) AS ro
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_external`
GROUP BY file
ORDER BY file;

#Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

SELECT count(DISTINCT(PULocationID))
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`;

SELECT
  COUNTIF(fare_amount = 0) AS zero_fares,
  COUNTIF(fare_amount IS NULL) AS null_fares
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`;


SELECT count(DISTINCT(PULocationID))
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_external`;

SELECT count(1)
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`
WHERE fare_amount =0;


#Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

SELECT PULocationID
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`;

SELECT PULocationID, DOLocationID
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`;

CREATE OR REPLACE TABLE `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`
);

#Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive).

SELECT DISTINCT VendorID
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' and '2024-03-15';

SELECT DISTINCT VendorID
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' and '2024-03-15';

SELECT COUNT(*) 
FROM `mythic-altar-485103-v8.my_zoomcamp_bigdataset.yellow_taxi`;


