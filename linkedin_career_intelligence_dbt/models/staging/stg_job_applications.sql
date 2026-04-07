select
    cast(application_date as timestamp) as application_date,
    nullif(trim(contact_email), '') as contact_email,
    nullif(trim(contact_phone_number), '') as contact_phone_number,
    trim(company_name) as company_name,
    trim(job_title) as job_title,
    nullif(trim(job_url), '') as job_url,
    nullif(trim(resume_name), '') as resume_name,
    nullif(trim(question_and_answers), '') as question_and_answers
from {{ source('bronze', 'job_applications') }}
