{{ config(materialized="table") }}

select
    service_date,
    coalesce(transport_category, 'UNKNOWN') as transport_category,
    departures_total,
    departures_canceled,
    avg_delay_seconds,
    delay_rate,
    on_time_rate,
    realtime_coverage
from {{ ref('mart_mobility_kpis_daily') }}
;
