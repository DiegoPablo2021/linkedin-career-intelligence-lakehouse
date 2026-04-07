select
    skill_name,
    trim(lower(skill_name)) as skill_name_clean,
    length(skill_name) as skill_name_length,
    case
        when lower(skill_name) like '%sql%'
            or lower(skill_name) like '%power bi%'
            or lower(skill_name) like '%tableau%'
            or lower(skill_name) like '%analytics%'
            or lower(skill_name) like '%data%'
            then 'analytics'
        when lower(skill_name) like '%python%'
            or lower(skill_name) like '%java%'
            or lower(skill_name) like '%c#%'
            or lower(skill_name) like '%javascript%'
            or lower(skill_name) like '%typescript%'
            then 'programming'
        when lower(skill_name) like '%azure%'
            or lower(skill_name) like '%aws%'
            or lower(skill_name) like '%cloud%'
            or lower(skill_name) like '%databricks%'
            then 'cloud_data'
        when lower(skill_name) like '%leadership%'
            or lower(skill_name) like '%communication%'
            or lower(skill_name) like '%comunica%'
            or lower(skill_name) like '%team%'
            then 'soft_skills'
        else 'other'
    end as skill_category
from {{ ref('stg_skills') }}
