select
    {{ trim_text('company_name') }} as company_name,
    {{ trim_text('title') }} as title,
    {{ nullif_trim_text('description') }} as description,
    {{ nullif_trim_text('location') }} as location,
    cast({{ nullif_trim_text('started_on') }} as date) as started_on,
    cast({{ nullif_trim_text('finished_on') }} as date) as finished_on,
    is_current
from {{ source('bronze', 'positions') }}
