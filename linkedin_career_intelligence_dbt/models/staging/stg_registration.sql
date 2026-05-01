select
    cast({{ nullif_trim_text('registered_at') }} as timestamp) as registered_at,
    {{ trim_text('registration_ip') }} as registration_ip,
    {{ nullif_trim_text('subscription_types') }} as subscription_types
from {{ source('bronze', 'registration') }}
