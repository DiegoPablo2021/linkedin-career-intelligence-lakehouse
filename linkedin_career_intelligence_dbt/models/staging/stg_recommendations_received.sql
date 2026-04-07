select
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    trim(company) as company,
    trim(position) as position,
    trim(recommendation_text) as recommendation_text,
    cast(recommendation_date as date) as recommendation_date,
    trim(visibility) as visibility
from {{ source('bronze', 'recommendations_received') }}