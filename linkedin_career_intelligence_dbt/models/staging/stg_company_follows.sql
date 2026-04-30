select
    {{ trim_text('organization') }} as organization,
    cast({{ nullif_trim_text('followed_on') }} as date) as followed_on
from {{ source('bronze', 'company_follows') }}
