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
        trim(school_name) as school_name,
        nullif(trim(degree_name), '') as degree_name,
        nullif(trim(notes), '') as notes,
        cast(started_on as date) as started_on,
        cast(finished_on as date) as finished_on,
        coalesce(is_current_education, false) as is_current_education
    from source_data

)

select *
from cleaned
