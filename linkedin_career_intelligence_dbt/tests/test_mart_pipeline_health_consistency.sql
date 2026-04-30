select *
from {{ ref('mart_pipeline_health_summary') }}
where total_inventory_files <> successful_reads + failed_reads
