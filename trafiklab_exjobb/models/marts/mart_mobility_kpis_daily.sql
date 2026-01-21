with base as (
    select *
    from {{ ref('fct_departure_delays') }}
)

select
    service_date,

    count(*) as departures_total,
    sum(case when canceled = 1 then 1 else 0 end) as departures_canceled,

    avg(delay_seconds) as avg_delay_seconds,
    avg(case when delay_seconds > 60 then 1 else 0 end) as delay_rate,

    avg(case when delay_seconds <= 60 then 1 else 0 end) as on_time_rate,

    avg(case when is_realtime = 1 then 1 else 0 end) as realtime_coverage

from base
group by service_date
