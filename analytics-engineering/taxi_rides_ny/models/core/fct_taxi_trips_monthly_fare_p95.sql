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