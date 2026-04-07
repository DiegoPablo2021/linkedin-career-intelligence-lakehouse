select
    organization_clean,
    count(*) as follow_count,
    min(followed_on) as first_follow_date,
    max(followed_on) as last_follow_date
from {{ ref('int_company_follows_enriched') }}
group by 1
order by follow_count desc, organization_clean