{{ config(materialized='table') }}

with base as (
    select
        service_date,
        route_key,
        route_designation,
        route_transport_mode,
        route_direction,
        transport_category,
        delay_seconds,
        canceled,
        is_realtime
    from {{ ref('fct_departure_delays') }}
),

agg as (
    select
        service_date,
        route_key,

        -- route_name does NOT exist in your dataset, so we keep a safe label:
        any_value(route_designation) as route_designation,
        any_value(route_transport_mode) as route_transport_mode,
        any_value(route_direction) as route_direction,

        coalesce(transport_category, 'UNKNOWN') as transport_category,

        count(*) as departures_total,
        sum(case when canceled then 1 else 0 end) as departures_canceled,

        avg(coalesce(delay_seconds, 0)) as avg_delay_seconds,

        -- delayed if > 60 seconds
        avg(case when coalesce(delay_seconds, 0) > 60 then 1 else 0 end) as delay_rate,

        -- on-time = not canceled AND delay <= 60
        avg(case when (not canceled) and coalesce(delay_seconds, 0) <= 60 then 1 else 0 end) as on_time_rate,

        -- realtime coverage
        avg(case when is_realtime then 1 else 0 end) as realtime_coverage

    from base
    group by
        service_date,
        route_key,
        coalesce(transport_category, 'UNKNOWN')
)

select * from agg

