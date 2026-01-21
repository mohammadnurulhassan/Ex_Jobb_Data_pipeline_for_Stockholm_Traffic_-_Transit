with base as (
    select *
    from {{ ref('fct_departure_delays') }}
)

select
    service_date,
    day_of_week,
    hour_of_day,
    coalesce(transport_category, 'UNKNOWN') as transport_category,

    count(*) as departures_total,
    avg(delay_seconds) as avg_delay_seconds,
    avg(case when delay_seconds > 60 then 1 else 0 end) as delay_rate,
    avg(case when delay_seconds <= 60 then 1 else 0 end) as on_time_rate

from base
group by service_date, day_of_week, hour_of_day, coalesce(transport_category, 'UNKNOWN')

