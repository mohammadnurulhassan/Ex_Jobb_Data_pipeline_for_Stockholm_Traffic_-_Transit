select
    agency_id,
    max(agency_name)     as agency_name,
    max(agency_operator) as agency_operator
from {{ source('trafiklab_raw', 'trafiklab_departures') }}
where agency_id is not null
group by agency_id
