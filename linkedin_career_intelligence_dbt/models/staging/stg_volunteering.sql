select
    {{ trim_text('company_name') }} as company_name,
    {{ trim_text('role') }} as role,
    {{ nullif_trim_text('cause') }} as cause,
    cast({{ nullif_trim_text('started_on') }} as date) as started_on,
    cast({{ nullif_trim_text('finished_on') }} as date) as finished_on,
    {{ nullif_trim_text('description') }} as description,
    cast(is_current_volunteering as boolean) as is_current_volunteering
from {{ source('bronze', 'volunteering') }}
