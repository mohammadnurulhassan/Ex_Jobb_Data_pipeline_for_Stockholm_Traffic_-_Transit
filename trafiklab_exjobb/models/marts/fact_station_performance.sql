-- Station-level performance metrics
{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    station_id,
    station_name,
    
    -- Overall metrics
    COUNT(*) AS total_departures,
    COUNT(DISTINCT line_number) AS unique_lines,
    COUNT(DISTINCT transport_mode) AS transport_modes,
    
    -- Delay statistics
    AVG(delay_minutes) AS avg_delay_minutes,
    MAX(delay_minutes) AS max_delay_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delay_minutes) AS median_delay_minutes,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delay_minutes) AS p95_delay_minutes,
    
    -- Delay categories
    SUM(CASE WHEN delay_minutes = 0 THEN 1 ELSE 0 END) AS on_time_departures,
    SUM(CASE WHEN delay_minutes BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS minor_delays,
    SUM(CASE WHEN delay_minutes BETWEEN 4 AND 10 THEN 1 ELSE 0 END) AS moderate_delays,
    SUM(CASE WHEN delay_minutes > 10 THEN 1 ELSE 0 END) AS major_delays,
    
    -- Percentages
    ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) AS overall_delay_rate,
    ROUND(100.0 * SUM(CASE WHEN delay_minutes = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS on_time_rate,
    
    -- By transport mode
    AVG(CASE WHEN transport_mode = 'METRO' THEN delay_minutes END) AS metro_avg_delay,
    AVG(CASE WHEN transport_mode = 'BUS' THEN delay_minutes END) AS bus_avg_delay,
    AVG(CASE WHEN transport_mode = 'TRAIN' THEN delay_minutes END) AS train_avg_delay,
    
    -- Rush hour analysis
    AVG(CASE WHEN is_morning_rush THEN delay_minutes END) AS morning_rush_avg_delay,
    AVG(CASE WHEN is_evening_rush THEN delay_minutes END) AS evening_rush_avg_delay,
    AVG(CASE WHEN NOT is_morning_rush AND NOT is_evening_rush THEN delay_minutes END) AS off_peak_avg_delay,
    
    -- Weekend vs weekday
    AVG(CASE WHEN is_weekend THEN delay_minutes END) AS weekend_avg_delay,
    AVG(CASE WHEN NOT is_weekend THEN delay_minutes END) AS weekday_avg_delay,
    
    -- Disruptions
    SUM(CASE WHEN has_deviation THEN 1 ELSE 0 END) AS total_deviations,
    ROUND(100.0 * SUM(CASE WHEN has_deviation THEN 1 ELSE 0 END) / COUNT(*), 2) AS deviation_rate,
    
    -- Time range
    MIN(expected_datetime) AS first_departure,
    MAX(expected_datetime) AS last_departure,
    
    CURRENT_TIMESTAMP AS created_at
    
FROM {{ ref('stg_departures') }}
WHERE expected_datetime >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY station_id, station_name