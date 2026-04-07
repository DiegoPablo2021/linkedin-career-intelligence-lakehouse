select
    start_year,
    start_month,
    start_year_month,

    count(*) as total_certifications,

    count(distinct name_clean) as unique_certifications,
    count(distinct authority_clean) as unique_authorities,

    avg(certification_duration_months) as avg_duration_months

from {{ ref('int_certifications_enriched') }}

group by
    start_year,
    start_month,
    start_year_month

order by
    start_year,
    start_month