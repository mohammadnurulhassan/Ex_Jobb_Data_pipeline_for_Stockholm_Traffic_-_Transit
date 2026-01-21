-- fct_departure_delays.sql
-- Delay analytics fact table (dashboard + ML ready)

with dep as (

    select
        departure_sk,
        response_timestamp,

        scheduled_time,
        realtime_time,
        delay_seconds,
        canceled,
        is_realtime,

        route_name,
        route_designation,
        route_transport_mode,
        route_transport_mode_code,
        route_direction,
        transport_category,

        origin_stop_id,
        destination_stop_id,

        stop_id,
        agency_id,

        platform_designation

    from {{ ref('stg_trafiklab_departures') }}

),

stops as (

    select
        stop_id,
        stop_name,
        stop_lat,
        stop_lon
    from {{ ref('stg_trafiklab_stops') }}

),

routes as (

    select
        route_key,
        route_designation,
        route_transport_mode,
        route_transport_mode_code,
        route_direction,
        transport_category
    from {{ ref('stg_trafiklab_routes') }}

),

agencies as (

    select
        agency_id,
        agency_name,
        agency_operator
    from {{ ref('stg_trafiklab_agencies') }}

),

enhanced as (

    select
        d.departure_sk,
        d.response_timestamp,

        d.scheduled_time,
        d.realtime_time,
        d.delay_seconds,
        d.canceled,
        d.is_realtime,

        -- time dimensions
        date(d.scheduled_time) as service_date,
        cast(strftime(d.scheduled_time, '%H') as integer) as hour_of_day,
        cast(strftime(d.scheduled_time, '%w') as integer) as day_of_week, -- 0=Sunday

        -- delay flags
        case
            when d.delay_seconds is null then null
            when d.delay_seconds > 60 then 1
            else 0
        end as is_delayed,

        -- stop attributes
        d.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,

        -- route attributes
        r.route_key,
        coalesce(r.route_designation, d.route_designation) as route_designation,
        coalesce(r.route_transport_mode, d.route_transport_mode) as route_transport_mode,
        coalesce(r.transport_category, d.transport_category) as transport_category,
        coalesce(r.route_direction, d.route_direction) as route_direction,

        -- agency attributes
        d.agency_id,
        a.agency_name,
        a.agency_operator,

        -- platform
        d.platform_designation

    from dep d
    left join stops s
        on d.stop_id = s.stop_id

    left join routes r
        on r.route_key = md5(
            coalesce(d.route_name,'') || '|' ||
            coalesce(d.route_designation,'') || '|' ||
            coalesce(d.route_transport_mode,'') || '|' ||
            coalesce(d.route_transport_mode_code,'') || '|' ||
            coalesce(d.route_direction,'') || '|' ||
            coalesce(d.origin_stop_id,'') || '|' ||
            coalesce(d.destination_stop_id,'')
        )

    left join agencies a
        on d.agency_id = a.agency_id

),

dedup as (

    select
        *,
        row_number() over (
            partition by departure_sk
            order by response_timestamp desc, realtime_time desc
        ) as rn
    from enhanced

)

select
    -- keep everything except rn
    * exclude (rn)
from dedup
where rn = 1


