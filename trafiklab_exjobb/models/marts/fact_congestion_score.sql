-- Calculated congestion scores
{{ config(
    materialized='table',
    schema='analytics'
) }}

WITH hourly_metrics AS (
    SELECT
        DATE_TRUNC('hour', expected_datetime) AS hour,
        station_id,
        station_name,
        
        -- Traffic volume
        COUNT(*) AS departure_count,
        COUNT(DISTINCT line_number) AS active_lines,
        
        -- Delay metrics
        AVG(delay_minutes) AS avg_delay,
        STDDEV(delay_minutes) AS delay_variance,
        MAX(delay_minutes) AS max_delay,
        
        -- Delayed vehicles
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_vehicles,
        
        -- By mode
        COUNT(CASE WHEN transport_mode = 'METRO' THEN 1 END) AS metro_count,
        COUNT(CASE WHEN transport_mode = 'BUS' THEN 1 END) AS bus_count,
        COUNT(CASE WHEN transport_mode = 'TRAIN' THEN 1 END) AS train_count,
        COUNT(CASE WHEN transport_mode = 'TRAM' THEN 1 END) AS tram_count,
        
        -- Deviations
        SUM(CASE WHEN has_deviation THEN 1 ELSE 0 END) AS disruption_count,
        
        -- Time flags
        BOOL_OR(is_morning_rush) AS is_morning_rush,
        BOOL_OR(is_evening_rush) AS is_evening_rush,
        BOOL_OR(is_weekend) AS is_weekend
        
    FROM {{ ref('stg_departures') }}
    WHERE expected_datetime >= CURRENT_TIMESTAMP - INTERVAL '60 days'
    GROUP BY 1, 2, 3
)

SELECT
    hour,
    station_id,
    station_name,
    departure_count,
    active_lines,
    avg_delay,
    delay_variance,
    max_delay,
    delayed_vehicles,
    metro_count,
    bus_count,
    train_count,
    tram_count,
    disruption_count,
    is_morning_rush,
    is_evening_rush,
    is_weekend,
    
    -- Congestion Score Calculation (0-100)
    -- Based on: delays, variance, volume, disruptions
    LEAST(100, GREATEST(0,
        (COALESCE(avg_delay, 0) * 7.0) +                    -- Avg delay impact
        (COALESCE(delay_variance, 0) * 3.0) +               -- Variability impact
        (CASE 
            WHEN departure_count > 80 THEN 20               -- High volume
            WHEN departure_count > 50 THEN 10
            ELSE 0 
        END) +
        (COALESCE(delayed_vehicles, 0) * 1.5) +             -- Delayed count impact
        (COALESCE(disruption_count, 0) * 5.0) +             -- Disruption impact
        (CASE WHEN max_delay > 15 THEN 10 ELSE 0 END)       -- Major delay bonus
    ))::INTEGER AS congestion_score,
    
    -- Congestion Level Category
    CASE
        WHEN LEAST(100, (COALESCE(avg_delay, 0) * 7.0) + (COALESCE(delay_variance, 0) * 3.0)) < 25 THEN 'Low'
        WHEN LEAST(100, (COALESCE(avg_delay, 0) * 7.0) + (COALESCE(delay_variance, 0) * 3.0)) < 50 THEN 'Moderate'
        WHEN LEAST(100, (COALESCE(avg_delay, 0) * 7.0) + (COALESCE(delay_variance, 0) * 3.0)) < 75 THEN 'High'
        ELSE 'Critical'
    END AS congestion_level,
    
    -- Traffic Status
    CASE
        WHEN avg_delay < 2 AND delayed_vehicles < 3 THEN 'Smooth'
        WHEN avg_delay < 5 AND delayed_vehicles < 10 THEN 'Normal'
        WHEN avg_delay < 8 THEN 'Congested'
        ELSE 'Heavy'
    END AS traffic_status,
    
    CURRENT_TIMESTAMP AS created_at
    
FROM hourly_metrics
