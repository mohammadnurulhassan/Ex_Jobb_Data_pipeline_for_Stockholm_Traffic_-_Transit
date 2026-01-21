with base as (
    select
        md5(
            coalesce(route_name,'') || '|' ||
            coalesce(route_designation,'') || '|' ||
            coalesce(route_transport_mode,'') || '|' ||
            coalesce(route_transport_mode_code,'') || '|' ||
            coalesce(route_direction,'') || '|' ||
            coalesce(origin_stop_id,'') || '|' ||
            coalesce(destination_stop_id,'')
        ) as route_key,

        route_name,
        route_designation,
        route_transport_mode,
        route_transport_mode_code,
        route_direction,
        transport_category,
        origin_stop_id,
        origin_stop_name,
        destination_stop_id,
        destination_stop_name
    from {{ source('trafiklab_raw', 'trafiklab_departures') }}
    where route_transport_mode is not null
)

select
    route_key,
    max(route_name) as route_name,
    max(route_designation) as route_designation,
    max(route_transport_mode) as route_transport_mode,
    max(route_transport_mode_code) as route_transport_mode_code,
    max(route_direction) as route_direction,
    max(transport_category) as transport_category,
    max(origin_stop_id) as origin_stop_id,
    max(origin_stop_name) as origin_stop_name,
    max(destination_stop_id) as destination_stop_id,
    max(destination_stop_name) as destination_stop_name
from base
group by route_key

