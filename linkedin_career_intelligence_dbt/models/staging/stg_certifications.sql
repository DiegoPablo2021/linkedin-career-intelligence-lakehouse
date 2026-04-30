select
    {{ trim_text('name') }} as name,
    {{ nullif_trim_text('url') }} as url,
    {{ nullif_trim_text('authority') }} as authority,
    cast({{ nullif_trim_text('started_on') }} as date) as started_on,
    cast({{ nullif_trim_text('finished_on') }} as date) as finished_on,
    {{ nullif_trim_text('license_number') }} as license_number

from {{ source('bronze', 'certifications') }}
