select
    {{ trim_text('first_name') }} as first_name,
    {{ trim_text('last_name') }} as last_name,
    {{ trim_text('first_name') }} || ' ' || {{ trim_text('last_name') }} as full_name,
    {{ trim_text('url') }} as profile_url,
    {{ nullif_trim_text('email_address') }} as email_address,
    {{ trim_text('company') }} as company,
    {{ trim_text('position') }} as position,
    cast({{ nullif_trim_text('connected_on') }} as date) as connected_on
from {{ source('bronze', 'connections') }}
