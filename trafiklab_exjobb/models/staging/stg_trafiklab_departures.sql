-- stg_trafiklab_departures.sql
-- বাংলা ধারণা: raw টেবিলকে একটু clean করে analytics-friendly বানাচ্ছি।

with src as (

    select
        -- metadata
        response_timestamp,
        query_time,
        query_area_id,

        -- timing
        scheduled_time,
        realtime_time,
        delay_seconds,
        canceled,
        is_realtime,

        -- route info
        route_name,
        route_designation,
        route_transport_mode_code,
        route_transport_mode,
        route_direction,

        -- origin / destination
        origin_stop_id,
        origin_stop_name,
        destination_stop_id,
        destination_stop_name,

        -- trip info
        trip_id,
        trip_start_date,
        trip_technical_number,

        -- agency
        agency_id,
        agency_name,
        agency_operator,

        -- stop
        stop_id,
        stop_name,
        stop_lat,
        stop_lon

    from {{ source('trafiklab_raw', 'trafiklab_departures') }}

)

select
    -- optional surrogate key (simple concat, no dbt_utils needed)
    concat_ws(
        '-',
        coalesce(trip_id, ''),
        coalesce(cast(scheduled_time as varchar), ''),
        coalesce(stop_id, '')
    ) as departure_sk,

    response_timestamp,
    query_time,
    query_area_id,
    scheduled_time,
    realtime_time,
    delay_seconds,
    canceled,
    is_realtime,
    route_name,
    route_designation,
    route_transport_mode_code,
    route_transport_mode,
    route_direction,
    origin_stop_id,
    origin_stop_name,
    destination_stop_id,
    destination_stop_name,
    trip_id,
    trip_start_date,
    trip_technical_number,
    agency_id,
    agency_name,
    agency_operator,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon

from src
