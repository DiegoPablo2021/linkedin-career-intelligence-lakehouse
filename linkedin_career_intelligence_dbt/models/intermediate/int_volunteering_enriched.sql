select
    company_name,
    role,
    cause,
    started_on,
    finished_on,
    description,
    is_current_volunteering,
    trim(lower(company_name)) as company_name_clean,
    trim(lower(role)) as role_clean,
    trim(lower(coalesce(cause, 'sem_causa'))) as cause_clean,
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
    ) as duration_months
from {{ ref('stg_volunteering') }}
