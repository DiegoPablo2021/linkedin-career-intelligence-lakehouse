select
    name,
    url,
    authority,
    started_on,
    finished_on,
    license_number,

    {{ lower_trim_text('name') }} as name_clean,
    {{ lower_trim_text('authority') }} as authority_clean,

    extract(year from started_on) as start_year,
    extract(month from started_on) as start_month,
    strftime(started_on, '%Y-%m') as start_year_month,

    extract(year from finished_on) as end_year,
    extract(month from finished_on) as end_month,
    strftime(finished_on, '%Y-%m') as end_year_month,

    case
        when finished_on is null then current_date
        else finished_on
    end as effective_end_date,

    date_diff(
        'month',
        started_on,
        case
            when finished_on is null then current_date
            else finished_on
        end
    ) as certification_duration_months,

    case
        when authority_clean like '%microsoft%' then 'microsoft'
        when authority_clean like '%aws%' then 'aws'
        when authority_clean like '%google%' then 'google'
        when authority_clean like '%oracle%' then 'oracle'
        else 'outros'
    end as certification_track

from {{ ref('stg_certifications') }}
