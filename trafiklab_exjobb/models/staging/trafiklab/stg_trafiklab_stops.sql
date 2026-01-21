select distinct
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from {{ source('trafiklab_raw', 'trafiklab_departures') }}
where stop_id is not null
