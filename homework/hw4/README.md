# Homework 4
## Question 5

fct_taxi_trips_quarterly_revenue.sql
```sql
WITH qtly_rev AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
        SUM(total_amount) AS quarterly_revenue
    FROM
        {{ ref('fact_trips') }}
    WHERE pickup_datetime >= '2019-01-01' AND pickup_datetime < '2021-01-01'
    GROUP BY
        year, quarter, service_type
)

SELECT
    year,
    quarter,
    service_type,
    quarterly_revenue,
    LAG(quarterly_revenue) OVER (
        PARTITION BY service_type, quarter
        ORDER BY year
    ) AS prev_year_quarterly_revenue,
    ROUND((quarterly_revenue - LAG(quarterly_revenue) OVER (
        PARTITION BY service_type, quarter
        ORDER BY year
    )) / NULLIF(LAG(quarterly_revenue) OVER (
        PARTITION BY service_type, quarter
        ORDER BY year
    ), 0) * 100, 2
    ) AS yoy_growth_percent
FROM
    qtly_rev
```

## Question 6

fct_taxi_trips_monthly_fare_p95.sql
```sql
WITH valid_trips AS (
SELECT
    service_type,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    fare_amount
FROM {{ ref('fact_trips') }}
WHERE 
    fare_amount > 0
    AND trip_distance > 0
    AND payment_type_description IN ('Cash', 'Credit card')
)

SELECT
  service_type,
  year,
  month,
  fare_amount,
  ROUND(PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month), 2) AS p90,
  ROUND(PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month), 2) AS p95,
  ROUND(PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month), 2) AS p97
FROM
  valid_trips
ORDER BY
  service_type,
  year,
  month,
  fare_amount
```

## Question 7

stg_fhv_tripdata.sql
```
{{
    config(
        materialized='view'
    )
}}

select *
from {{ source('staging', 'fhv_tripdata') }}
where dispatching_base_num is not null
```

dim_fhv_trips.sql
```
with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropOff_datetime,
    EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS year,
    EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS month,
    fhv_tripdata.PUlocationID,
    fhv_tripdata.DOlocationID,
    fhv_tripdata.SR_Flag,
    fhv_tripdata.Affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.DOlocationID = dropoff_zone.locationid
```

fct_fhv_monthly_zone_traveltime_p90.sql
```
WITH fhv_trip_durations AS (
SELECT 
    *,
    TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, SECOND) AS trip_duration
FROM {{ ref('dim_fhv_trips') }}
)

SELECT 
    *,
    ROUND(PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, PUlocationID, DOlocationID), 2) AS p90
FROM fhv_trip_durations
```