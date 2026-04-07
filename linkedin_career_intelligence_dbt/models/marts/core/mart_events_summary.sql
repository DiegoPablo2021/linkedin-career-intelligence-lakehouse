select
    event_year,
    event_month,
    event_year_month,
    status,
    count(*) as total_events,
    sum(case when has_external_url then 1 else 0 end) as events_with_url
from {{ ref('int_events_enriched') }}
group by event_year, event_month, event_year_month, status
order by event_year, event_month, status
