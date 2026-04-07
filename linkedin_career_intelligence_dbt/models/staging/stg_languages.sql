select
    trim(name) as name,
    trim(proficiency) as proficiency
from {{ source('bronze', 'languages') }}