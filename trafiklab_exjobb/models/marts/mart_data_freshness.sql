select
    max(response_timestamp) as latest_response_timestamp,
    now() as now_ts,
    datediff('minute', max(response_timestamp), now()) as minutes_since_last_update
from {{ ref('stg_trafiklab_departures') }}
