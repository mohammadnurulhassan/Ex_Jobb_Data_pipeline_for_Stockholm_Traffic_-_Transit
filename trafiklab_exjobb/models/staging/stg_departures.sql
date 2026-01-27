{{ config(materialized='view', schema='analytics_staging') }}

with source as (
    select *
    from {{ source('raw_traffic', 'realtime_departures') }}
),

base as (
    select
        try_cast(s.site_id as bigint) as station_id,
        try_cast(s.site_name as varchar) as station_name,

        -- stop point (prefer flattened)
        try_cast(coalesce(s.stop_point__name, s.stop_point__designation, s.stop_point) as varchar) as stop_point_name,

        -- line fields (prefer flattened)
        try_cast(coalesce(s.line__designation, s.line) as varchar) as line_designation,
        try_cast(s.line__group_of_lines as varchar) as line_group,
        try_cast(coalesce(s.line__transport_mode, s.transport_mode) as varchar) as transport_mode,

        try_cast(s.destination as varchar) as destination,
        try_cast(s.direction as varchar) as direction,

        try_cast(s.expected_datetime as timestamptz) as expected_datetime,
        try_cast(s.scheduled_datetime as timestamptz) as scheduled_datetime,

        try_cast(s.ingestion_timestamp_utc as timestamptz) as ingestion_timestamp,

        try_cast(s.deviations_raw as varchar) as deviations_raw,
        try_cast(s.has_deviation as boolean) as has_deviation_raw,

        s._dlt_load_id,
        s._dlt_id

    from source s
),

features as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(station_id as varchar)',
            'coalesce(stop_point_name, '''')',
            'coalesce(line_designation, '''')',
            'coalesce(destination, '''')',
            'cast(expected_datetime as varchar)',
            'cast(scheduled_datetime as varchar)',
            'cast(ingestion_timestamp as varchar)'
        ]) }} as departure_id,

        station_id,
        station_name,
        stop_point_name,

        line_designation,
        line_group,
        transport_mode,

        destination,
        direction,

        expected_datetime,
        scheduled_datetime,
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

        -- cleaned deviation text
        case
            when deviations_raw is null then null
            when lower(deviations_raw) in ('[]', '', 'none', 'null') then null
            else deviations_raw
        end as deviation_text,

        -- ✅ final has_deviation column (robust)
        coalesce(
            has_deviation_raw,
            case when deviation_text is not null then true else false end
        ) as has_deviation,

        extract(hour from expected_datetime) as departure_hour,
        extract(dow from expected_datetime)  as day_of_week,
        extract(year from expected_datetime) as year,
        extract(month from expected_datetime) as month,
        extract(day from expected_datetime)  as day,

        case when extract(hour from expected_datetime) between 7 and 9 then true else false end as is_morning_rush,
        case when extract(hour from expected_datetime) between 16 and 18 then true else false end as is_evening_rush,
        case when extract(dow from expected_datetime) in (0, 6) then true else false end as is_weekend,

        _dlt_load_id,
        _dlt_id

    from base
    where expected_datetime is not null
      and scheduled_datetime is not null
      and station_id is not null
)

select *
from features
where delay_minutes between -10 and 60

