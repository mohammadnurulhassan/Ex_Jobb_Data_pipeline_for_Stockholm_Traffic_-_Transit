-- fct_departure_delays.sql
-- delay নিয়ে analytics fact টেবিল, dashboard + ML ready

with base as (

    select
        departure_sk,
        scheduled_time,
        realtime_time,
        delay_seconds,
        route_designation,
        route_transport_mode,
        transport_category,
        stop_id,
        stop_name
    from {{ ref('stg_trafiklab_departures') }}

),

enhanced as (

    select
        departure_sk,
        scheduled_time,
        realtime_time,
        delay_seconds,

        route_designation,
        route_transport_mode,
        transport_category,

        stop_id,
        stop_name,

        -- time dimensions
        date(scheduled_time) as service_date,
        strftime(scheduled_time, '%H')::integer as hour_of_day,
        strftime(scheduled_time, '%w')::integer as day_of_week, -- 0=Sunday

        -- delay flags
        case 
            when delay_seconds is null then null
            when delay_seconds > 60 then 1
            else 0
        end as is_delayed

    from base
)

select *
from enhanced
