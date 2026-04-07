select
    sender_name,
    recipient_name,
    sent_at,
    message,
    direction,
    inviter_profile_url,
    invitee_profile_url,
    case when message is not null then true else false end as has_message,
    extract(year from sent_at) as invitation_year,
    extract(month from sent_at) as invitation_month,
    strftime(sent_at, '%Y-%m') as invitation_year_month
from {{ ref('stg_invitations') }}
