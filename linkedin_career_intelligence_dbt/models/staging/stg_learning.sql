select
    trim(content_title) as content_title,
    nullif(trim(content_description), '') as content_description,
    nullif(trim(content_type), '') as content_type,
    cast(last_watched_at as timestamp) as last_watched_at,
    cast(completed_at as timestamp) as completed_at,
    cast(content_saved as boolean) as content_saved,
    nullif(trim(notes_taken), '') as notes_taken
from {{ source('bronze', 'learning') }}
