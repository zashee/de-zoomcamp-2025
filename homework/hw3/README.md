# Homework 3
## BQ Setup
```sql
CREATE OR REPLACE EXTERNAL TABLE `annular-ocean-447506-s1.ny_taxi_dataset.ext_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://annular-ocean-447506-s1-ny-taxi-bucket/raw/yellow_tripdata_2024-*.parquet']
);

-- create non-partitioned table
CREATE OR REPLACE TABLE `annular-ocean-447506-s1.ny_taxi_dataset.fhv_yellow_tripdata` AS
SELECT * FROM ny_taxi_dataset.ext_yellow_tripdata;

-- create partitioned table
CREATE OR REPLACE TABLE `annular-ocean-447506-s1.ny_taxi_dataset.fhv_yellow_tripdata_partitoned`
PARTITION BY
  DATE(tpep_dropoff_datetime) AS
SELECT * FROM ny_taxi_dataset.ext_yellow_tripdata;
```

## Question 1
```sql
SELECT COUNT(*) FROM `ny_taxi_dataset.ext_yellow_tripdata`;
```

## Question 2
```sql
SELECT COUNT(DISTINCT(PULocationID))FROM `ny_taxi_dataset.ext_yellow_tripdata`;
SELECT COUNT(DISTINCT(PULocationID))FROM `ny_taxi_dataset.fhv_yellow_tripdata`;
```

## Question 3
```sql
SELECT PULocationID FROM `ny_taxi_dataset.fhv_yellow_tripdata`;
SELECT PULocationID, DOLocationID FROM `ny_taxi_dataset.fhv_yellow_tripdata`;
```

## Question 4
```sql
SELECT COUNT(*) FROM `ny_taxi_dataset.fhv_yellow_tripdata` WHERE fare_amount = 0;
```

## Question 6
```sql
-- non-partitioned table
SELECT DISTINCT(VendorID)
FROM ny_taxi_dataset.fhv_yellow_tripdata
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- partitioned table
SELECT DISTINCT(VendorID)
FROM ny_taxi_dataset.fhv_yellow_tripdata_partitoned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```