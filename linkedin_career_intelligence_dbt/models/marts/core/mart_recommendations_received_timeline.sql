select
    extract(year from recommendation_date) as recommendation_year,
    extract(month from recommendation_date) as recommendation_month,
    strftime(recommendation_date, '%Y-%m') as recommendation_year_month,
    count(*) as total_recommendations,
    avg(text_length) as avg_text_length,
    sum(mentions_data) as mentions_data_count,
    sum(mentions_teamwork) as mentions_teamwork_count
from {{ ref('int_recommendations_received_enriched') }}
where recommendation_date is not null
group by
    recommendation_year,
    recommendation_month,
    recommendation_year_month
order by
    recommendation_year,
    recommendation_month

