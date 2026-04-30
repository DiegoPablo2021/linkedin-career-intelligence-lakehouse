select
    {{ trim_text('first_name') }} as first_name,
    {{ trim_text('last_name') }} as last_name,
    {{ trim_text('maiden_name') }} as maiden_name,
    {{ trim_text('address') }} as address,

    cast({{ nullif_trim_text('birth_date') }} as date) as birth_date,

    {{ trim_text('headline') }} as headline,
    {{ trim_text('summary') }} as summary,
    {{ trim_text('industry') }} as industry,
    {{ trim_text('zip_code') }} as zip_code,
    {{ trim_text('geo_location') }} as geo_location,
    {{ trim_text('twitter_handles') }} as twitter_handles,
    {{ trim_text('websites') }} as websites,
    {{ trim_text('instant_messengers') }} as instant_messengers

from {{ source('bronze', 'profile') }}
