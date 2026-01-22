{{ config(materialized="table") }}

with base as (
    select
        service_date,
        stop_id,
        stop_name,
        transport_category,
        departures_total,
        departures_canceled,
        avg_delay_seconds,
        delay_rate,
        on_time_rate,
        realtime_coverage
    from {{ ref('fct_departure_delays') }}
)

select
    service_date,
    stop_id,
    any_value(stop_name) as stop_name,
    coalesce(transport_category, 'UNKNOWN') as transport_category,

    sum(departures_total) as departures_total,
    sum(departures_canceled) as departures_canceled,

    -- weighted avg delay by departures
    case when sum(departures_total) = 0 then null
         else sum(avg_delay_seconds * departures_total) / sum(departures_total)
    end as avg_delay_seconds,

    -- weighted rates by departures
    case when sum(departures_total) = 0 then null
         else sum(delay_rate * departures_total) / sum(departures_total)
    end as delay_rate,

    case when sum(departures_total) = 0 then null
         else sum(on_time_rate * departures_total) / sum(departures_total)
    end as on_time_rate,

    case when sum(departures_total) = 0 then null
         else sum(realtime_coverage * departures_total) / sum(departures_total)
    end as realtime_coverage

from base
group by 1,2,4
;
