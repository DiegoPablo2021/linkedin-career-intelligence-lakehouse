select
    start_year,
    start_month,
    start_year_month,

    count(*) as total_education_started,
    sum(case when is_current_education then 1 else 0 end) as current_education_started,
    count(distinct school_name_clean) as unique_schools,
    count(distinct degree_name_clean) as unique_degrees,
    avg(education_duration_months) as avg_education_duration_months

from {{ ref('int_education_enriched') }}

group by
    start_year,
    start_month,
    start_year_month

order by
    start_year,
    start_month