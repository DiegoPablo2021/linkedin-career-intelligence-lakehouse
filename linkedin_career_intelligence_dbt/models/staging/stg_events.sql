select
    {{ trim_text('event_name') }} as event_name,
    {{ trim_text('event_time') }} as event_time,
    {{ upper_trim_text('status') }} as status,
    {{ nullif_trim_text('external_url') }} as external_url,
    cast({{ nullif_trim_text('started_at') }} as timestamp) as started_at,
    cast({{ nullif_trim_text('finished_at') }} as timestamp) as finished_at
from {{ source('bronze', 'events') }}
