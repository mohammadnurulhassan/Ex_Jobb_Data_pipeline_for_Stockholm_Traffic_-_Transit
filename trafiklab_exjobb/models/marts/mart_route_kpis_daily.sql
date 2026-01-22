{{ config(materialized="table") }}

with base as (
    select
        service_date,
        route_key,
        route_name,
        route_designation,
        transport_category,
        departures_total,
        departures_canceled,
        avg_delay_seconds,
        delay_rate,
        on_time_rate
    from {{ ref('fct_departure_delays') }}
)

select
    service_date,
    route_key,
    any_value(route_name) as route_name,
    any_value(route_designation) as route_designation,
    coalesce(transport_category, 'UNKNOWN') as transport_category,

    sum(departures_total) as departures_total,
    sum(departures_canceled) as departures_canceled,

    case when sum(departures_total) = 0 then null
         else sum(avg_delay_seconds * departures_total) / sum(departures_total)
    end as avg_delay_seconds,

    case when sum(departures_total) = 0 then null
         else sum(delay_rate * departures_total) / sum(departures_total)
    end as delay_rate,

    case when sum(departures_total) = 0 then null
         else sum(on_time_rate * departures_total) / sum(departures_total)
    end as on_time_rate

from base
group by 1,2,5
;
