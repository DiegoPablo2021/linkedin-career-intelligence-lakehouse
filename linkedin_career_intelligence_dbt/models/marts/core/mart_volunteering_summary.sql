select
    cause_clean,
    count(*) as total_volunteering_roles,
    count(distinct company_name_clean) as unique_organizations,
    sum(case when is_current_volunteering then 1 else 0 end) as current_roles,
    round(avg(duration_months), 2) as avg_duration_months
from {{ ref('int_volunteering_enriched') }}
group by cause_clean
order by total_volunteering_roles desc, cause_clean
