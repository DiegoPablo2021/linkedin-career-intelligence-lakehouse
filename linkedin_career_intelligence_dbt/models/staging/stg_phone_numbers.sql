select
    {{ nullif_trim_text('extension') }} as extension,
    {{ trim_text('phone_number') }} as phone_number,
    {{ nullif_trim_text('phone_type') }} as phone_type
from {{ source('bronze', 'phone_numbers') }}
