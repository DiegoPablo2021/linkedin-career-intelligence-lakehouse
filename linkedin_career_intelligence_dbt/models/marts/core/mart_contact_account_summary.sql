with emails as (
    select
        count(*) as total_email_addresses,
        sum(case when confirmed then 1 else 0 end) as confirmed_email_addresses,
        sum(case when is_primary then 1 else 0 end) as primary_email_addresses
    from {{ ref('stg_email_addresses') }}
),
phones as (
    select count(*) as total_phone_numbers
    from {{ ref('stg_phone_numbers') }}
),
registration as (
    select
        min(registered_at) as first_registered_at,
        max(registered_at) as latest_registered_at,
        max(subscription_types) as subscription_types
    from {{ ref('stg_registration') }}
)

select
    emails.total_email_addresses,
    emails.confirmed_email_addresses,
    emails.primary_email_addresses,
    phones.total_phone_numbers,
    registration.first_registered_at,
    registration.latest_registered_at,
    registration.subscription_types,
    date_diff(
        'year',
        cast(registration.first_registered_at as date),
        current_date
    ) as account_age_years
from emails
cross join phones
cross join registration
