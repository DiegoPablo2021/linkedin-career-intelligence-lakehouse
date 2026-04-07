select
    nullif(trim(extension), '') as extension,
    trim(phone_number) as phone_number,
    nullif(trim(phone_type), '') as phone_type
from {{ source('bronze', 'phone_numbers') }}
