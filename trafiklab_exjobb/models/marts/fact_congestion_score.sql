{{ config(materialized='table', schema='analytics_marts') }}

with hourly_metrics as (
    select
        date_trunc('hour', expected_datetime) as hour,
        station_id,
        station_name,

        count(*) as departure_count,
        count(distinct line_designation) as active_lines,

        avg(delay_minutes) as avg_delay,
        stddev_samp(delay_minutes) as delay_variance,
        max(delay_minutes) as max_delay,

        sum(case when is_delayed then 1 else 0 end) as delayed_vehicles,

        sum(case when transport_mode = 'METRO' then 1 else 0 end) as metro_count,
        sum(case when transport_mode = 'BUS'   then 1 else 0 end) as bus_count,
        sum(case when transport_mode = 'TRAIN' then 1 else 0 end) as train_count,
        sum(case when transport_mode = 'TRAM'  then 1 else 0 end) as tram_count,

        sum(case when has_deviation then 1 else 0 end) as disruption_count,

        bool_or(is_morning_rush) as is_morning_rush,
        bool_or(is_evening_rush) as is_evening_rush,
        bool_or(is_weekend) as is_weekend

    from {{ ref('stg_departures') }}
    where expected_datetime >= current_timestamp - interval '60 days'
    group by 1,2,3
)

select
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

    least(100, greatest(0,
        (coalesce(avg_delay, 0) * 7.0) +
        (coalesce(delay_variance, 0) * 3.0) +
        (case when departure_count > 80 then 20 when departure_count > 50 then 10 else 0 end) +
        (coalesce(delayed_vehicles, 0) * 1.5) +
        (coalesce(disruption_count, 0) * 5.0) +
        (case when coalesce(max_delay, 0) > 15 then 10 else 0 end)
    ))::integer as congestion_score,

    case
        when least(100, (coalesce(avg_delay, 0) * 7.0) + (coalesce(delay_variance, 0) * 3.0)) < 25 then 'Low'
        when least(100, (coalesce(avg_delay, 0) * 7.0) + (coalesce(delay_variance, 0) * 3.0)) < 50 then 'Moderate'
        when least(100, (coalesce(avg_delay, 0) * 7.0) + (coalesce(delay_variance, 0) * 3.0)) < 75 then 'High'
        else 'Critical'
    end as congestion_level,

    case
        when coalesce(avg_delay, 0) < 2 and coalesce(delayed_vehicles, 0) < 3 then 'Smooth'
        when coalesce(avg_delay, 0) < 5 and coalesce(delayed_vehicles, 0) < 10 then 'Normal'
        when coalesce(avg_delay, 0) < 8 then 'Congested'
        else 'Heavy'
    end as traffic_status,

    current_timestamp as created_at

from hourly_metrics;


