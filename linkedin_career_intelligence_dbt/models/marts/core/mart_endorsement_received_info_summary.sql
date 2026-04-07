select
    skill_name_clean,
    count(*) as endorsement_count,
    count(distinct endorser_full_name) as unique_endorsers,
    min(endorsement_date) as first_endorsement_date,
    max(endorsement_date) as last_endorsement_date
from {{ ref('int_endorsement_received_info_enriched') }}
group by 1
order by endorsement_count desc, skill_name_clean