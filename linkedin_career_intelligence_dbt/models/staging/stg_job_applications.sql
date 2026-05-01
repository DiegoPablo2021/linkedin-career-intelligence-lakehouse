select
    cast({{ nullif_trim_text('application_date') }} as timestamp) as application_date,
    {{ nullif_trim_text('contact_email') }} as contact_email,
    {{ nullif_trim_text('contact_phone_number') }} as contact_phone_number,
    {{ trim_text('company_name') }} as company_name,
    {{ trim_text('job_title') }} as job_title,
    {{ nullif_trim_text('job_url') }} as job_url,
    {{ nullif_trim_text('resume_name') }} as resume_name,
    {{ nullif_trim_text('question_and_answers') }} as question_and_answers
from {{ source('bronze', 'job_applications') }}
