-- Calculated congestion scores
{{ config(
    materialized='table',
    schema='analytics_marts'
) }}

WITH hourly_metrics AS (
    SELECT
        date_trunc('hour', expected_datetime) AS hour,
        station_id,
        station_name,

        -- Traffic volume
        count(*) AS departure_count,
        count(distinct line_designation) AS active_lines,

        -- Delay metrics
        avg(delay_minutes) AS avg_delay,
        stddev_samp(delay_minutes) AS delay_variance,
        max(delay_minutes) AS max_delay,

        -- Delayed vehicles
        sum(case when is_delayed then 1 else 0 end) AS delayed_vehicles,

        -- By mode (DuckDB-safe)
        sum(case when transport_mode = 'METRO' then 1 else 0 end) AS metro_count,
        sum(case when transport_mode = 'BUS'   then 1 else 0 end) AS bus_count,
        sum(case when transport_mode = 'TRAIN' then 1 else 0 end) AS train_count,
        sum(case when transport_mode = 'TRAM'  then 1 else 0 end) AS tram_count,

        -- Deviations
        sum(case when has_deviation then 1 else 0 end) AS disruption_count,

        -- Time flags
        bool_or(is_morning_rush) AS is_morning_rush,
        bool_or(is_evening_rush) AS is_evening_rush,
        bool_or(is_weekend) AS is_weekend

    FROM {{ ref('stg_departures') }}
    WHERE expected_datetime >= current_timestamp - interval '60 days'
      and delay_minutes is not null
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
    least(100, greatest(0,
        (coalesce(avg_delay, 0) * 7.0) +                    -- Avg delay impact
        (coalesce(delay_variance, 0) * 3.0) +               -- Variability impact
        (case
            when departure_count > 80 then 20               -- High volume
            when departure_count > 50 then 10
            else 0
        end) +
        (coalesce(delayed_vehicles, 0) * 1.5) +             -- Delayed count impact
        (coalesce(disruption_count, 0) * 5.0) +             -- Disruption impact
        (case when coalesce(max_delay, 0) > 15 then 10 else 0 end)  -- Major delay bonus
    ))::integer AS congestion_score,

    -- Congestion Level Category
    case
        when least(100, (coalesce(avg_delay, 0) * 7.0) + (coalesce(delay_variance, 0) * 3.0)) < 25 then 'Low'
        when least(100, (coalesce(avg_delay, 0) * 7.0) + (coalesce(delay_variance, 0) * 3.0)) < 50 then 'Moderate'
        when least(100, (coalesce(avg_delay, 0) * 7.0) + (coalesce(delay_variance, 0) * 3.0)) < 75 then 'High'
        else 'Critical'
    end AS congestion_level,

    -- Traffic Status
    case
        when coalesce(avg_delay, 0) < 2 and coalesce(delayed_vehicles, 0) < 3 then 'Smooth'
        when coalesce(avg_delay, 0) < 5 and coalesce(delayed_vehicles, 0) < 10 then 'Normal'
        when coalesce(avg_delay, 0) < 8 then 'Congested'
        else 'Heavy'
    end AS traffic_status,

    current_timestamp AS created_at

FROM hourly_metrics

