{{ config(materialized='table', schema='analytics_marts') }}

select
    station_id,
    station_name,

    count(*) as total_departures,
    count(distinct line_designation) as unique_lines,
    count(distinct transport_mode) as transport_modes,

    avg(delay_minutes) as avg_delay_minutes,
    max(delay_minutes) as max_delay_minutes,
    quantile_cont(delay_minutes, 0.5) as median_delay_minutes,
    quantile_cont(delay_minutes, 0.95) as p95_delay_minutes,

    sum(case when delay_minutes <= 0 then 1 else 0 end) as on_time_or_early_departures,
    sum(case when delay_minutes between 1 and 3 then 1 else 0 end) as minor_delays,
    sum(case when delay_minutes between 4 and 10 then 1 else 0 end) as moderate_delays,
    sum(case when delay_minutes > 10 then 1 else 0 end) as major_delays,

    round(100.0 * sum(case when is_delayed then 1 else 0 end) / count(*), 2) as overall_delay_rate,
    round(100.0 * sum(case when delay_minutes <= 0 then 1 else 0 end) / count(*), 2) as on_time_rate,

    avg(case when transport_mode = 'METRO' then delay_minutes end) as metro_avg_delay,
    avg(case when transport_mode = 'BUS' then delay_minutes end) as bus_avg_delay,
    avg(case when transport_mode = 'TRAIN' then delay_minutes end) as train_avg_delay,
    avg(case when transport_mode = 'TRAM' then delay_minutes end) as tram_avg_delay,

    avg(case when is_morning_rush then delay_minutes end) as morning_rush_avg_delay,
    avg(case when is_evening_rush then delay_minutes end) as evening_rush_avg_delay,
    avg(case when not is_morning_rush and not is_evening_rush then delay_minutes end) as off_peak_avg_delay,

    avg(case when is_weekend then delay_minutes end) as weekend_avg_delay,
    avg(case when not is_weekend then delay_minutes end) as weekday_avg_delay,

    sum(case when has_deviation then 1 else 0 end) as total_deviations,
    round(100.0 * sum(case when has_deviation then 1 else 0 end) / count(*), 2) as deviation_rate,

    min(expected_datetime) as first_departure,
    max(expected_datetime) as last_departure,

    current_timestamp as created_at

from {{ ref('stg_departures') }}
where expected_datetime >= current_timestamp - interval '30 days'
group by station_id, station_name
