select
    trim(organization) as organization,
    cast(followed_on as date) as followed_on
from {{ source('bronze', 'company_follows') }}