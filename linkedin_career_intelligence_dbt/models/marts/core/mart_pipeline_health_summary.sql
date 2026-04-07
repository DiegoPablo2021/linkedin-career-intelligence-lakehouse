with inventory as (
    select
        count(*) as total_inventory_files,
        sum(case when read_success then 1 else 0 end) as successful_reads,
        sum(case when not read_success then 1 else 0 end) as failed_reads,
        max(inventory_timestamp) as latest_inventory_timestamp
    from {{ ref('stg_file_inventory') }}
),
connections as (
    select count(*) as total_connections
    from {{ ref('int_connections_enriched') }}
),
positions as (
    select count(*) as total_positions
    from {{ ref('int_positions_enriched') }}
),
education as (
    select count(*) as total_education_records
    from {{ ref('int_education_enriched') }}
),
certifications as (
    select count(*) as total_certifications
    from {{ ref('int_certifications_enriched') }}
),
languages as (
    select count(*) as total_languages
    from {{ ref('int_languages_enriched') }}
),
endorsements as (
    select count(*) as total_endorsements
    from {{ ref('int_endorsement_received_info_enriched') }}
),
company_follows as (
    select count(*) as total_company_follows
    from {{ ref('int_company_follows_enriched') }}
),
recommendations as (
    select count(*) as total_recommendations
    from {{ ref('int_recommendations_received_enriched') }}
),
skills as (
    select count(*) as total_skills
    from {{ ref('int_skills_enriched') }}
),
invitations as (
    select count(*) as total_invitations
    from {{ ref('int_invitations_enriched') }}
),
events as (
    select count(*) as total_events
    from {{ ref('int_events_enriched') }}
),
learning as (
    select count(*) as total_learning_records
    from {{ ref('int_learning_enriched') }}
),
job_applications as (
    select count(*) as total_job_applications
    from {{ ref('int_job_applications_enriched') }}
),
saved_job_alerts as (
    select count(*) as total_saved_job_alerts
    from {{ ref('int_saved_job_alerts_enriched') }}
),
volunteering as (
    select count(*) as total_volunteering
    from {{ ref('int_volunteering_enriched') }}
),
emails as (
    select count(*) as total_email_addresses
    from {{ ref('stg_email_addresses') }}
),
phones as (
    select count(*) as total_phone_numbers
    from {{ ref('stg_phone_numbers') }}
)

select
    inventory.total_inventory_files,
    inventory.successful_reads,
    inventory.failed_reads,
    inventory.latest_inventory_timestamp,
    connections.total_connections,
    positions.total_positions,
    education.total_education_records,
    certifications.total_certifications,
    languages.total_languages,
    endorsements.total_endorsements,
    company_follows.total_company_follows,
    recommendations.total_recommendations,
    skills.total_skills,
    invitations.total_invitations,
    events.total_events,
    learning.total_learning_records,
    job_applications.total_job_applications,
    saved_job_alerts.total_saved_job_alerts,
    volunteering.total_volunteering,
    emails.total_email_addresses,
    phones.total_phone_numbers
from inventory
cross join connections
cross join positions
cross join education
cross join certifications
cross join languages
cross join endorsements
cross join company_follows
cross join recommendations
cross join skills
cross join invitations
cross join events
cross join learning
cross join job_applications
cross join saved_job_alerts
cross join volunteering
cross join emails
cross join phones
