select
    trim(event_name) as event_name,
    trim(event_time) as event_time,
    upper(trim(status)) as status,
    nullif(trim(external_url), '') as external_url,
    cast(started_at as timestamp) as started_at,
    cast(finished_at as timestamp) as finished_at
from {{ source('bronze', 'events') }}
