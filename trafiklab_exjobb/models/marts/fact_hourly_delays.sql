{{ config(materialized='table', schema='analytics_marts') }}

select
    date_trunc('hour', expected_datetime) as hour,
    station_id,
    station_name,
    line_designation,
    transport_mode,

    count(*) as total_departures,
    count(distinct destination) as unique_destinations,
    sum(case when is_delayed then 1 else 0 end) as delayed_departures,

    avg(delay_minutes) as avg_delay_minutes,
    max(delay_minutes) as max_delay_minutes,
    min(delay_minutes) as min_delay_minutes,
    quantile_cont(delay_minutes, 0.5) as median_delay_minutes,
    stddev_samp(delay_minutes) as stddev_delay_minutes,

    round(100.0 * sum(case when is_delayed then 1 else 0 end) / count(*), 2) as delay_percentage,

    sum(case when has_deviation then 1 else 0 end) as departures_with_deviations,

    bool_or(is_morning_rush) as is_morning_rush_hour,
    bool_or(is_evening_rush) as is_evening_rush_hour,

    min(ingestion_timestamp) as first_recorded,
    max(ingestion_timestamp) as last_recorded,
    current_timestamp as created_at

from {{ ref('stg_departures') }}
where expected_datetime >= current_timestamp - interval '90 days'
group by 1,2,3,4,5




