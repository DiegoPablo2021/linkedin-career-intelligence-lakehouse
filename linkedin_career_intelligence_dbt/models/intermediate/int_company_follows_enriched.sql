select
    organization,
    lower(organization) as organization_clean,
    followed_on,

    extract(year from followed_on) as year,
    extract(month from followed_on) as month,

    strftime(followed_on, '%Y-%m') as year_month

from {{ ref('stg_company_follows') }}