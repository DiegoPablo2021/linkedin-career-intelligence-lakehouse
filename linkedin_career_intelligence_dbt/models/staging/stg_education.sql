with source_data as (

    select
        school_name,
        degree_name,
        notes,
        started_on,
        finished_on,
        is_current_education
    from {{ source('bronze', 'education') }}

),

cleaned as (

    select
        {{ trim_text('school_name') }} as school_name,
        {{ nullif_trim_text('degree_name') }} as degree_name,
        {{ nullif_trim_text('notes') }} as notes,
        cast({{ nullif_trim_text('started_on') }} as date) as started_on,
        cast({{ nullif_trim_text('finished_on') }} as date) as finished_on,
        coalesce(is_current_education, false) as is_current_education
    from source_data

)

select *
from cleaned
