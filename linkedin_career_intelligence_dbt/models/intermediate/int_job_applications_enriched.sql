select
    application_date,
    contact_email,
    contact_phone_number,
    company_name,
    job_title,
    job_url,
    resume_name,
    question_and_answers,
    trim(lower(company_name)) as company_name_clean,
    trim(lower(job_title)) as job_title_clean,
    case when resume_name is not null then true else false end as has_resume,
    case when question_and_answers is not null then true else false end as has_questionnaire,
    extract(year from application_date) as application_year,
    extract(month from application_date) as application_month,
    strftime(application_date, '%Y-%m') as application_year_month,
    case
        when lower(job_title) like '%data%' then 'data'
        when lower(job_title) like '%analista%' then 'analyst'
        when lower(job_title) like '%engenheir%' then 'engineering'
        when lower(job_title) like '%develop%' then 'development'
        else 'other'
    end as job_family
from {{ ref('stg_job_applications') }}
