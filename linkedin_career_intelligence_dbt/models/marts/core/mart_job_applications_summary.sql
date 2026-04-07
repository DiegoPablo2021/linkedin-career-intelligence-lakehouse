select
    application_year,
    application_month,
    application_year_month,
    job_family,
    count(*) as total_applications,
    count(distinct company_name_clean) as unique_companies,
    sum(case when has_resume then 1 else 0 end) as applications_with_resume,
    sum(case when has_questionnaire then 1 else 0 end) as applications_with_questionnaire
from {{ ref('int_job_applications_enriched') }}
group by application_year, application_month, application_year_month, job_family
order by application_year, application_month, job_family
