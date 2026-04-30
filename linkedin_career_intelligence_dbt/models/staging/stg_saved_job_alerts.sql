select
    {{ trim_text('alert_parameters') }} as alert_parameters,
    {{ trim_text('query_context') }} as query_context,
    {{ trim_text('saved_search_id') }} as saved_search_id
from {{ source('bronze', 'saved_job_alerts') }}
