select 
    -- identifiers
    cast(vendorid as int) as vendor_id,
    cast(ratecodeid as int) as rate_code_id,
    cast(pulocationid as int) as pu_location_id,
    cast(dolocationid as int) do_location_id,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    1 as trip_type, -- yellow taxi are always street hail. do trip_type = 1 always

    -- payment details
    cast(fare_amount as numeric) as fare_amount, 
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount, 
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as int) as payment_type,
    0 as ehail_fee --yellow taxi ehail fee is always 0

from {{ source('raw_data', 'yellow_tripdata')}}
where vendorid is not null