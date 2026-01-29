{{ config(materialized='view') }}

with src as (
    select
        -- ingestion timestamp (dashboard uses this for "last 5 minutes")
        try_cast(ingestion_timestamp_utc as timestamptz) as ingestion_timestamp,

        -- station/site
        try_cast(site_id as bigint) as station_id,
        cast(site_name as varchar) as station_name,

        -- line (prefer nested designation if present)
        coalesce(
            nullif(cast(line__designation as varchar), ''),
            nullif(cast(line as varchar), ''),
            'UNKNOWN'
        ) as line_designation,

        -- transport mode (prefer main transport_mode)
        upper(coalesce(
            nullif(cast(transport_mode as varchar), ''),
            nullif(cast(line__transport_mode as varchar), ''),
            'UNKNOWN'
        )) as transport_mode,

        -- destination
        cast(destination as varchar) as destination,

        -- timestamps
        try_cast(expected_datetime as timestamptz)  as expected_datetime,
        try_cast(scheduled_datetime as timestamptz) as scheduled_datetime,

        -- deviation flag (already boolean)
        coalesce(has_deviation, false) as has_deviation,

        -- raw deviations text if you want later
        cast(deviations_raw as varchar) as deviations_raw

    from {{ source('raw_traffic', 'realtime_departures') }}
),

features as (
    select
        *,
           md5(
          cast(station_id as varchar) || '|' ||
          coalesce(line_designation, '') || '|' ||
          coalesce(destination, '') || '|' ||
          cast(expected_datetime as varchar) || '|' ||
          cast(scheduled_datetime as varchar)
        ) as departure_id,


        -- compute delay in minutes (expected - scheduled)
        -- NOTE: positive = delayed, negative = early
        date_diff('minute', scheduled_datetime, expected_datetime) as delay_minutes,

        -- delayed flag used by marts
        (date_diff('minute', scheduled_datetime, expected_datetime) > 0) as is_delayed,

        -- rush flags
        (extract('hour' from expected_datetime) between 7 and 9) as is_morning_rush,
        (extract('hour' from expected_datetime) between 16 and 18) as is_evening_rush,

        -- weekend (DuckDB: dow 0=Sunday, 6=Saturday)
        (extract('dow' from expected_datetime) in (0, 6)) as is_weekend

    from src
    where expected_datetime is not null
      and scheduled_datetime is not null
)

select * from features


