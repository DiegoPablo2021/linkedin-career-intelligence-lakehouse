select
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    trim(maiden_name) as maiden_name,
    trim(address) as address,

    cast(birth_date as date) as birth_date,

    trim(headline) as headline,
    trim(summary) as summary,
    trim(industry) as industry,
    trim(zip_code) as zip_code,
    trim(geo_location) as geo_location,
    trim(twitter_handles) as twitter_handles,
    trim(websites) as websites,
    trim(instant_messengers) as instant_messengers

from {{ source('bronze', 'profile') }}