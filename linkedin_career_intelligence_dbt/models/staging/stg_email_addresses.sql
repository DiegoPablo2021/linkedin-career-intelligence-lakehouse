select
    trim(email_address) as email_address,
    cast(confirmed as boolean) as confirmed,
    cast("primary" as boolean) as is_primary,
    nullif(trim(updated_on), '') as updated_on
from {{ source('bronze', 'email_addresses') }}
