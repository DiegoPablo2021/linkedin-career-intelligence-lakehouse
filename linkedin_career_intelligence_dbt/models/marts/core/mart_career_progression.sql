select
    start_year,
    start_month,
    start_year_month,

    count(*) as total_positions_started,
    sum(case when is_current then 1 else 0 end) as current_positions_started,
    count(distinct company_name_clean) as unique_companies,
    count(distinct title_clean) as unique_titles,
    avg(duration_months) as avg_duration_months

from {{ ref('int_positions_enriched') }}

group by
    start_year,
    start_month,
    start_year_month

order by
    start_year,
    start_month