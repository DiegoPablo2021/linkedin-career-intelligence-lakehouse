select
    content_title,
    content_description,
    content_type,
    last_watched_at,
    completed_at,
    content_saved,
    notes_taken,
    trim(lower(content_title)) as content_title_clean,
    trim(lower(coalesce(content_type, 'outros'))) as content_type_clean,
    case when completed_at is not null then true else false end as is_completed,
    case when notes_taken is not null then true else false end as has_notes,
    strftime(coalesce(completed_at, last_watched_at), '%Y-%m') as engagement_year_month
from {{ ref('stg_learning') }}
