select
    {{ trim_text('email_address') }} as email_address,
    cast(confirmed as boolean) as confirmed,
    cast("primary" as boolean) as is_primary,
    {{ nullif_trim_text('updated_on') }} as updated_on
from {{ source('bronze', 'email_addresses') }}
