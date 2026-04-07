select
    count(*) as total_recommendations,
    avg(text_length) as avg_text_length,
    sum(mentions_data) as mentions_data_count,
    sum(mentions_teamwork) as mentions_teamwork_count,
    min(recommendation_date) as first_recommendation_date,
    max(recommendation_date) as last_recommendation_date
from {{ ref('int_recommendations_received_enriched') }}