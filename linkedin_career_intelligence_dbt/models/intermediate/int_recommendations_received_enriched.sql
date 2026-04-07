select
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as recommender_name,
    company,
    position,
    recommendation_text,
    recommendation_date,
    visibility,

    length(recommendation_text) as text_length,

    case
        when lower(recommendation_text) like '%data%' then 1
        else 0
    end as mentions_data,

    case
        when lower(recommendation_text) like '%team%' then 1
        else 0
    end as mentions_teamwork

from {{ ref('stg_recommendations_received') }}