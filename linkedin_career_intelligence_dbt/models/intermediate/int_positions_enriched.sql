select
    company_name,
    title,
    description,
    location,
    started_on,
    finished_on,
    is_current,

    trim(lower(company_name)) as company_name_clean,
    trim(lower(title)) as title_clean,
    trim(lower(coalesce(location, ''))) as location_clean,

    extract(year from started_on) as start_year,
    extract(month from started_on) as start_month,
    strftime(started_on, '%Y-%m') as start_year_month,

    extract(year from finished_on) as end_year,
    extract(month from finished_on) as end_month,
    strftime(finished_on, '%Y-%m') as end_year_month,

    case
        when is_current then current_date
        else finished_on
    end as effective_end_date,

    date_diff('month', started_on, case when is_current then current_date else finished_on end) as duration_months,

    case
        when title_clean like '%analista%' then 'analista'
        when title_clean like '%engenheiro%' then 'engenharia'
        when title_clean like '%programador%' then 'desenvolvimento'
        when title_clean like '%consultor%' then 'consultoria'
        when title_clean like '%assistente%' then 'assistente'
        else 'outros'
    end as career_track

from {{ ref('stg_positions') }}