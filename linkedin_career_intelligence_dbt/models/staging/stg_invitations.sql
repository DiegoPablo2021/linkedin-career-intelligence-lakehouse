select
    trim(sender_name) as sender_name,
    trim(recipient_name) as recipient_name,
    cast(sent_at as timestamp) as sent_at,
    nullif(trim(message), '') as message,
    upper(trim(direction)) as direction,
    nullif(trim(inviter_profile_url), '') as inviter_profile_url,
    nullif(trim(invitee_profile_url), '') as invitee_profile_url
from {{ source('bronze', 'invitations') }}
