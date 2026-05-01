select
    {{ trim_text('name') }} as name,
    {{ trim_text('proficiency') }} as proficiency
from {{ source('bronze', 'languages') }}
