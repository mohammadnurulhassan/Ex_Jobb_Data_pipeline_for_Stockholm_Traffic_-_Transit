-- Station-level performance metrics
{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

SELECT
    station_id,
    station_name,

    -- Overall metrics
    count(*) AS total_departures,
    count(distinct line_designation) AS unique_lines,
    count(distinct transport_mode) AS transport_modes,

    -- Delay statistics
    avg(delay_minutes) AS avg_delay_minutes,
    max(delay_minutes) AS max_delay_minutes,
    quantile_cont(delay_minutes, 0.5) AS median_delay_minutes,
    quantile_cont(delay_minutes, 0.95) AS p95_delay_minutes,

    -- Delay categories
    sum(case when delay_minutes = 0 then 1 else 0 end) AS on_time_departures,
    sum(case when delay_minutes between 1 and 3 then 1 else 0 end) AS minor_delays,
    sum(case when delay_minutes between 4 and 10 then 1 else 0 end) AS moderate_delays,
    sum(case when delay_minutes > 10 then 1 else 0 end) AS major_delays,

    -- Percentages
    round(100.0 * sum(case when is_delayed then 1 else 0 end) / count(*), 2) AS overall_delay_rate,
    round(100.0 * sum(case when delay_minutes = 0 then 1 else 0 end) / count(*), 2) AS on_time_rate,

    -- By transport mode
    avg(case when transport_mode = 'METRO' then delay_minutes end) AS metro_avg_delay,
    avg(case when transport_mode = 'BUS' then delay_minutes end) AS bus_avg_delay,
    avg(case when transport_mode = 'TRAIN' then delay_minutes end) AS train_avg_delay,

    -- Rush hour analysis
    avg(case when is_morning_rush then delay_minutes end) AS morning_rush_avg_delay,
    avg(case when is_evening_rush then delay_minutes end) AS evening_rush_avg_delay,
    avg(case when not is_morning_rush and not is_evening_rush then delay_minutes end) AS off_peak_avg_delay,

    -- Weekend vs weekday
    avg(case when is_weekend then delay_minutes end) AS weekend_avg_delay,
    avg(case when not is_weekend then delay_minutes end) AS weekday_avg_delay,

    -- Disruptions
    sum(case when has_deviation then 1 else 0 end) AS total_deviations,
    round(100.0 * sum(case when has_deviation then 1 else 0 end) / count(*), 2) AS deviation_rate,

    -- Time range
    min(expected_datetime) AS first_departure,
    max(expected_datetime) AS last_departure,

    current_timestamp AS created_at

FROM {{ ref('stg_departures') }}
WHERE expected_datetime >= current_timestamp - interval '30 days'
  and delay_minutes is not null
GROUP BY station_id, station_name
