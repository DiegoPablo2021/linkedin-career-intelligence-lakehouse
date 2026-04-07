select
    trim(company_name) as company_name,
    trim(title) as title,
    nullif(trim(description), '') as description,
    nullif(trim(location), '') as location,
    cast(started_on as date) as started_on,
    cast(finished_on as date) as finished_on,
    is_current
from {{ source('bronze', 'positions') }}