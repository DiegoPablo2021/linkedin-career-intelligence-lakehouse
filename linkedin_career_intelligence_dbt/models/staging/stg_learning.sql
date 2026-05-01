select
    {{ trim_text('content_title') }} as content_title,
    {{ nullif_trim_text('content_description') }} as content_description,
    {{ nullif_trim_text('content_type') }} as content_type,
    cast({{ nullif_trim_text('last_watched_at') }} as timestamp) as last_watched_at,
    cast({{ nullif_trim_text('completed_at') }} as timestamp) as completed_at,
    cast(content_saved as boolean) as content_saved,
    {{ nullif_trim_text('notes_taken') }} as notes_taken
from {{ source('bronze', 'learning') }}
