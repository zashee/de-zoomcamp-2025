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