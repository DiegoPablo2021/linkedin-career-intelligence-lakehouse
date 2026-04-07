select
    endorsement_date,
    skill_name,
    lower(skill_name) as skill_name_clean,
    endorser_first_name,
    endorser_last_name,
    concat(
        coalesce(trim(endorser_first_name), ''),
        case
            when endorser_first_name is not null and endorser_last_name is not null then ' '
            else ''
        end,
        coalesce(trim(endorser_last_name), '')
    ) as endorser_full_name,
    endorser_public_url,
    endorsement_status,

    case
        when lower(endorsement_status) like '%accepted%' then 'accepted'
        when lower(endorsement_status) like '%pending%' then 'pending'
        when lower(endorsement_status) like '%rejected%' then 'rejected'
        else 'other'
    end as endorsement_status_group

from {{ ref('stg_endorsement_received_info') }}