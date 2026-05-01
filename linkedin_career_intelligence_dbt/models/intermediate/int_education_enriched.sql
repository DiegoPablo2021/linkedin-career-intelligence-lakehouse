select
    school_name,
    degree_name,
    notes,
    started_on,
    finished_on,
    is_current_education,

    {{ lower_trim_text('school_name') }} as school_name_clean,
    {{ lower_trim_text("coalesce(degree_name, '')") }} as degree_name_clean,

    extract(year from started_on) as start_year,
    extract(month from started_on) as start_month,
    strftime(started_on, '%Y-%m') as start_year_month,

    extract(year from finished_on) as end_year,
    extract(month from finished_on) as end_month,
    strftime(finished_on, '%Y-%m') as end_year_month,

    case
        when is_current_education then current_date
        else finished_on
    end as effective_end_date,

    date_diff(
        'month',
        started_on,
        case
            when is_current_education then current_date
            else finished_on
        end
    ) as education_duration_months,

    case
        when degree_name_clean like '%tecnólogo%' then 'tecnologo'
        when degree_name_clean like '%tecnologo%' then 'tecnologo'
        when degree_name_clean like '%bacharel%' then 'bacharelado'
        when degree_name_clean like '%licenciatura%' then 'licenciatura'
        when degree_name_clean like '%pós%' then 'pos_graduacao'
        when degree_name_clean like '%pos%' then 'pos_graduacao'
        when degree_name_clean like '%mba%' then 'mba'
        when degree_name_clean like '%mestrado%' then 'mestrado'
        when degree_name_clean like '%doutorado%' then 'doutorado'
        else 'outros'
    end as education_track

from {{ ref('stg_education') }}
