# Homework 1
## Question 3. Trip Segmentation Count
```postgresql
SELECT
	COUNT(CASE WHEN trip_distance <= 1 THEN 1 END) AS cnt_up_to_1_mile,
	COUNT(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 END) AS cnt_btwn_1_and_3_miles,
	COUNT(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 END) AS cnt_btwn_3_and_7_miles,
	COUNT(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 END) AS cnt_btwn_7_and_10_miles,
	COUNT(CASE WHEN trip_distance > 10 THEN 1 END) AS cnt_over_10_miles
FROM green_taxi_data
WHERE lpep_dropoff_datetime >= '2019-10-01'
  AND lpep_dropoff_datetime < '2019-11-01'
```

## Question 4. Longest trip for each day
```postgresql
WITH max_trip_distance AS (
    SELECT MAX(trip_distance) AS max_distance
    FROM green_taxi_data
)

SELECT g.lpep_pickup_datetime
FROM green_taxi_data g
JOIN max_trip_distance m
ON g.trip_distance = m.max_distance;
```

## Question 5. Three biggest pickup zones
```postgresql
SELECT
	z."Zone",
	SUM(g.total_amount) AS sum_total_amount
FROM
	green_taxi_data g
	INNER JOIN zones z ON g."PULocationID" = z."LocationID"
WHERE
	CAST(g.lpep_pickup_datetime AS date) = '2019-10-18'
GROUP BY
	z."Zone"
HAVING
	SUM(total_amount) > 13000
ORDER BY
	sum_total_amount DESC
```

## Question 6. Largest tip
```postgresql
SELECT
    dz."Zone" AS dropoff_zone,
    g."tip_amount"
FROM
    green_taxi_data g
    INNER JOIN zones pz ON g."PULocationID" = pz."LocationID"
    INNER JOIN zones dz ON g."DOLocationID" = dz."LocationID"
WHERE
    pz."Zone" = 'East Harlem North'
    AND g."lpep_pickup_datetime" >= '2019-10-01'
    AND g."lpep_pickup_datetime" < '2019-11-01'
ORDER BY
    g."tip_amount" DESC
LIMIT 1;
```