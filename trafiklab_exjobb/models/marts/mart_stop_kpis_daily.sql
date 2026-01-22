{{ config(materialized='table') }}

with base as (
    select
        service_date,
        stop_id,
        stop_name,
        transport_category,
        delay_seconds,
        canceled,
        is_realtime
    from {{ ref('fct_departure_delays') }}
),

agg as (
    select
        service_date,
        stop_id,
        any_value(stop_name) as stop_name,
        coalesce(transport_category, 'UNKNOWN') as transport_category,

        count(*) as departures_total,
        sum(case when canceled then 1 else 0 end) as departures_canceled,

        avg(coalesce(delay_seconds, 0)) as avg_delay_seconds,
        avg(case when coalesce(delay_seconds, 0) > 60 then 1 else 0 end) as delay_rate,
        avg(case when (not canceled) and coalesce(delay_seconds, 0) <= 60 then 1 else 0 end) as on_time_rate,
        avg(case when is_realtime then 1 else 0 end) as realtime_coverage

    from base
    group by
        service_date,
        stop_id,
        coalesce(transport_category, 'UNKNOWN')
)

select * from agg

