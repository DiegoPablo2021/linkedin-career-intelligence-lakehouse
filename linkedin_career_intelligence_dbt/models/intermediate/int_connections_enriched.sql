select
    first_name,
    last_name,
    full_name,
    profile_url,
    email_address,
    company,
    position,
    connected_on,

    case
        when email_address is not null then true
        else false
    end as has_email,

    trim(lower(company)) as company_clean,
    trim(lower(position)) as position_clean,

    extract(year from connected_on) as connection_year,
    extract(month from connected_on) as connection_month,
    strftime(connected_on, '%Y-%m') as connection_year_month

from {{ ref('stg_connections') }}