select
    coalesce(alert_frequency, 'unknown') as alert_frequency,
    count(*) as total_alerts,
    count(distinct search_keywords) as unique_keyword_sets,
    sum(case when has_remote_filter then 1 else 0 end) as remote_alerts,
    sum(case when has_company_filter then 1 else 0 end) as company_scoped_alerts
from {{ ref('int_saved_job_alerts_enriched') }}
group by coalesce(alert_frequency, 'unknown')
order by total_alerts desc, alert_frequency
