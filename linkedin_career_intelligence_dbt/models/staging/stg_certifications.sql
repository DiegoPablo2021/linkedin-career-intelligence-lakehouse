select
    trim(name) as name,
    nullif(trim(url), '') as url,
    nullif(trim(authority), '') as authority,
    cast(started_on as date) as started_on,
    cast(finished_on as date) as finished_on,
    nullif(trim(license_number), '') as license_number

from {{ source('bronze', 'certifications') }}