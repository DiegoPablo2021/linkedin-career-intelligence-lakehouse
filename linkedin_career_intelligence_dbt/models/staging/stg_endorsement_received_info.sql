select
    cast(endorsement_date as date) as endorsement_date,
    trim(skill_name) as skill_name,
    trim(endorser_first_name) as endorser_first_name,
    trim(endorser_last_name) as endorser_last_name,
    trim(endorser_public_url) as endorser_public_url,
    trim(endorsement_status) as endorsement_status
from {{ source('bronze', 'endorsement_received_info') }}