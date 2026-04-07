select
    trim(alert_parameters) as alert_parameters,
    trim(query_context) as query_context,
    trim(saved_search_id) as saved_search_id
from {{ source('bronze', 'saved_job_alerts') }}
