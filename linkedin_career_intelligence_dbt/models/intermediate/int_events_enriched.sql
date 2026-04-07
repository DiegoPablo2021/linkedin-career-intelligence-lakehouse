select
    event_name,
    event_time,
    status,
    external_url,
    started_at,
    finished_at,
    case when external_url is not null then true else false end as has_external_url,
    extract(year from started_at) as event_year,
    extract(month from started_at) as event_month,
    strftime(started_at, '%Y-%m') as event_year_month
from {{ ref('stg_events') }}
