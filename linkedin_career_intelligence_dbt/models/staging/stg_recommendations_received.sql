select
    {{ trim_text('first_name') }} as first_name,
    {{ trim_text('last_name') }} as last_name,
    {{ trim_text('company') }} as company,
    {{ trim_text('position') }} as position,
    {{ trim_text('recommendation_text') }} as recommendation_text,
    cast({{ nullif_trim_text('recommendation_date') }} as date) as recommendation_date,
    {{ trim_text('visibility') }} as visibility
from {{ source('bronze', 'recommendations_received') }}
