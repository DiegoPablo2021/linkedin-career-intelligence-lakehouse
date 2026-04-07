select
    trim(company_name) as company_name,
    trim(role) as role,
    nullif(trim(cause), '') as cause,
    cast(started_on as date) as started_on,
    cast(finished_on as date) as finished_on,
    nullif(trim(description), '') as description,
    cast(is_current_volunteering as boolean) as is_current_volunteering
from {{ source('bronze', 'volunteering') }}
