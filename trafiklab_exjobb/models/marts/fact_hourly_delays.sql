-- Hourly delay aggregations
{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

SELECT
    date_trunc('hour', expected_datetime) AS hour,
    station_id,
    station_name,
    line_designation,
    transport_mode,

    -- Counts
    count(*) AS total_departures,
    count(distinct destination) AS unique_destinations,
    sum(case when is_delayed then 1 else 0 end) AS delayed_departures,

    -- Delay metrics
    avg(delay_minutes) AS avg_delay_minutes,
    max(delay_minutes) AS max_delay_minutes,
    min(delay_minutes) AS min_delay_minutes,
    quantile_cont(delay_minutes, 0.5) AS median_delay_minutes,
    stddev_samp(delay_minutes) AS stddev_delay_minutes,

    -- Percentages
    round(100.0 * sum(case when is_delayed then 1 else 0 end) / count(*), 2) AS delay_percentage,

    -- Deviations
    sum(case when has_deviation then 1 else 0 end) AS departures_with_deviations,

    -- Rush hour flag
    bool_or(is_morning_rush) AS is_morning_rush_hour,
    bool_or(is_evening_rush) AS is_evening_rush_hour,

    -- Metadata
    min(ingestion_timestamp) AS first_recorded,
    max(ingestion_timestamp) AS last_recorded,
    current_timestamp AS created_at

FROM {{ ref('stg_departures') }}
WHERE expected_datetime >= current_timestamp - interval '90 days'
  and delay_minutes is not null
GROUP BY 1, 2, 3, 4, 5




