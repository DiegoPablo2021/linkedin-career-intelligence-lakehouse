select
    saved_search_id,
    alert_parameters,
    query_context,
    {{ nullif_trim_text("regexp_extract(query_context, 'keywords=([^,}]+)', 1)") }} as search_keywords,
    lower({{ nullif_trim_text("regexp_extract(alert_parameters, 'frequency=([A-Z]+)', 1)") }}) as alert_frequency,
    case
        when lower(query_context) like '%workplacetype:2%' then true
        else false
    end as has_remote_filter,
    case
        when lower(query_context) like '%organizations=%' then true
        else false
    end as has_company_filter
from {{ ref('stg_saved_job_alerts') }}
