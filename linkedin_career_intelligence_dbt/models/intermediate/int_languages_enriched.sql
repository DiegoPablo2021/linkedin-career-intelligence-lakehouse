select
    name,
    proficiency,

    trim(lower(name)) as language_name_clean,
    trim(lower(proficiency)) as proficiency_clean,

    case
        when proficiency_clean like '%native%' then 'nativo'
        when proficiency_clean like '%fluent%' then 'fluente'
        when proficiency_clean like '%professional%' then 'profissional'
        when proficiency_clean like '%full professional%' then 'profissional'
        when proficiency_clean like '%working%' then 'intermediario_profissional'
        when proficiency_clean like '%elementary%' then 'basico'
        when proficiency_clean like '%limited%' then 'basico'
        else 'outros'
    end as proficiency_track

from {{ ref('stg_languages') }}