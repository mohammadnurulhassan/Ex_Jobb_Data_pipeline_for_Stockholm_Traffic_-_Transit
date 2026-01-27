{{ config(materialized='view', schema='analytics_staging') }}

with source as (
    select * 
    from {{ source('raw_traffic', 'realtime_departures') }}
),

base as (
    select
        try_cast(s.site_id as bigint)                as station_id,
        try_cast(s.station_name as varchar)          as station_name,
        try_cast(s.stop_area_name as varchar)        as stop_area_name,

        try_cast(s.line_number as varchar)           as line_number,
        try_cast(s.destination as varchar)           as destination,
        try_cast(s.transport_mode as varchar)        as transport_mode,
        try_cast(s.group_of_line as varchar)         as group_of_line,
        try_cast(s.journey_direction as integer)     as journey_direction,

        try_cast(s.expected_datetime as timestamp)   as expected_datetime,
        try_cast(s.timetabled_datetime as timestamp) as scheduled_datetime,

        try_cast(s.display_time as varchar)          as display_time,
        try_cast(s.ingestion_timestamp as timestamp) as ingestion_timestamp,

        try_cast(s.deviations as varchar)            as deviations_raw

    from source s
),

features as (
    select
        row_number() over (
            order by ingestion_timestamp, station_id, destination, line_number
        ) as departure_id,

        station_id,
        station_name,
        stop_area_name,

        line_number,
        destination,
        transport_mode,
        group_of_line,
        journey_direction,

        expected_datetime,
        scheduled_datetime,
        display_time,
        ingestion_timestamp,

        case
            when expected_datetime is not null and scheduled_datetime is not null
                then date_diff('minute', scheduled_datetime, expected_datetime)
            else null
        end as delay_minutes,

        case
            when expected_datetime is not null and scheduled_datetime is not null
                 and date_diff('minute', scheduled_datetime, expected_datetime) > 3
                then true
            else false
        end as is_delayed,

        case
            when deviations_raw is null then false
            when lower(deviations_raw) in ('[]', '', 'none', 'null') then false
            else true
        end as has_deviation,

        case
            when deviations_raw is null then null
            when lower(deviations_raw) in ('[]', '', 'none', 'null') then null
            else deviations_raw
        end as deviation_text,

        extract(hour from expected_datetime) as departure_hour,
        extract(dow from expected_datetime)  as day_of_week,
        extract(year from expected_datetime) as year,
        extract(month from expected_datetime) as month,
        extract(day from expected_datetime)  as day,

        case when extract(hour from expected_datetime) between 7 and 9 then true else false end as is_morning_rush,
        case when extract(hour from expected_datetime) between 16 and 18 then true else false end as is_evening_rush,
        case when extract(dow from expected_datetime) in (0, 6) then true else false end as is_weekend

    from base
    where expected_datetime is not null
      and station_id is not null
)

select * from features

