select *
from {{ ref('mart_connections_summary') }}
where connections_with_email > total_connections
