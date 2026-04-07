select
    content_type_clean,
    count(*) as total_contents,
    sum(case when is_completed then 1 else 0 end) as completed_contents,
    sum(case when content_saved then 1 else 0 end) as saved_contents,
    sum(case when has_notes then 1 else 0 end) as contents_with_notes
from {{ ref('int_learning_enriched') }}
group by content_type_clean
order by total_contents desc, content_type_clean
