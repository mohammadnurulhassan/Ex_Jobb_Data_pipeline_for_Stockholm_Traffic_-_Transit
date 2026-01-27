-- Hourly delay aggregations
{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    DATE_TRUNC('hour', expected_datetime) AS hour,
    station_id,
    station_name,
    line_number,
    transport_mode,
    
    -- Counts
    COUNT(*) AS total_departures,
    COUNT(DISTINCT destination) AS unique_destinations,
    SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_departures,
    
    -- Delay metrics
    AVG(delay_minutes) AS avg_delay_minutes,
    MAX(delay_minutes) AS max_delay_minutes,
    MIN(delay_minutes) AS min_delay_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delay_minutes) AS median_delay_minutes,
    STDDEV(delay_minutes) AS stddev_delay_minutes,
    
    -- Percentages
    ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) AS delay_percentage,
    
    -- Deviations
    SUM(CASE WHEN has_deviation THEN 1 ELSE 0 END) AS departures_with_deviations,
    
    -- Rush hour flag
    BOOL_OR(is_morning_rush) AS is_morning_rush_hour,
    BOOL_OR(is_evening_rush) AS is_evening_rush_hour,
    
    -- Metadata
    MIN(ingestion_timestamp) AS first_recorded,
    MAX(ingestion_timestamp) AS last_recorded,
    CURRENT_TIMESTAMP AS created_at
    
FROM {{ ref('stg_departures') }}
WHERE expected_datetime >= CURRENT_TIMESTAMP - INTERVAL '90 days'
GROUP BY 1, 2, 3, 4, 5

