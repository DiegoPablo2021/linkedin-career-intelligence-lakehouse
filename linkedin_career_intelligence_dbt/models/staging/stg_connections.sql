select
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    trim(first_name) || ' ' || trim(last_name) as full_name,
    trim(url) as profile_url,
    nullif(trim(email_address), '') as email_address,
    trim(company) as company,
    trim(position) as position,
    cast(connected_on as date) as connected_on
from {{ source('bronze', 'connections') }}