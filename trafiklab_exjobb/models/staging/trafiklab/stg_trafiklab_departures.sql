with src as (
    select *
    from {{ source('trafiklab_raw', 'trafiklab_departures') }}
)

select
    trip_id,
    stop_id,
    scheduled_time,

    md5(
      coalesce(trip_id,'') || '|' ||
      coalesce(stop_id,'') || '|' ||
      cast(scheduled_time as varchar)
    ) as departure_sk,

    response_timestamp,
    query_time,
    query_area_id,
    realtime_time,
    delay_seconds,
    canceled,
    is_realtime,

    scheduled_platform__id           as scheduled_platform_id,
    scheduled_platform__designation  as scheduled_platform_designation,
    realtime_platform__id            as realtime_platform_id,
    realtime_platform__designation   as realtime_platform_designation,

    coalesce(realtime_platform__designation, scheduled_platform__designation) as platform_designation,

    route_name,
    route_designation,
    route_transport_mode,
    route_transport_mode_code,
    route_direction,
    transport_category,

    origin_stop_id,
    origin_stop_name,
    destination_stop_id,
    destination_stop_name,

    trip_start_date,
    trip_technical_number,

    agency_id,
    agency_name,
    agency_operator,

    stop_name,
    stop_lat,
    stop_lon,

    _dlt_id,
    _dlt_load_id

from src
where trip_id is not null
  and stop_id is not null
  and scheduled_time is not null
