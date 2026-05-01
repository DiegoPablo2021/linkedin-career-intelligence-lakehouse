select
    cast({{ nullif_trim_text('endorsement_date') }} as date) as endorsement_date,
    {{ trim_text('skill_name') }} as skill_name,
    {{ trim_text('endorser_first_name') }} as endorser_first_name,
    {{ trim_text('endorser_last_name') }} as endorser_last_name,
    {{ trim_text('endorser_public_url') }} as endorser_public_url,
    {{ trim_text('endorsement_status') }} as endorsement_status
from {{ source('bronze', 'endorsement_received_info') }}
