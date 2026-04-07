select
    invitation_year,
    invitation_month,
    invitation_year_month,
    direction,
    count(*) as total_invitations,
    sum(case when has_message then 1 else 0 end) as invitations_with_message
from {{ ref('int_invitations_enriched') }}
group by invitation_year, invitation_month, invitation_year_month, direction
order by invitation_year, invitation_month, direction
