select
    {{ trim_text('sender_name') }} as sender_name,
    {{ trim_text('recipient_name') }} as recipient_name,
    cast({{ nullif_trim_text('sent_at') }} as timestamp) as sent_at,
    {{ nullif_trim_text('message') }} as message,
    {{ upper_trim_text('direction') }} as direction,
    {{ nullif_trim_text('inviter_profile_url') }} as inviter_profile_url,
    {{ nullif_trim_text('invitee_profile_url') }} as invitee_profile_url
from {{ source('bronze', 'invitations') }}
