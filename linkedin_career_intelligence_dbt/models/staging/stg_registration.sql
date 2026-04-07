select
    cast(registered_at as timestamp) as registered_at,
    trim(registration_ip) as registration_ip,
    nullif(trim(subscription_types), '') as subscription_types
from {{ source('bronze', 'registration') }}
