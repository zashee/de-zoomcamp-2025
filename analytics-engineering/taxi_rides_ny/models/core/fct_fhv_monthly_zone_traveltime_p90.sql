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

