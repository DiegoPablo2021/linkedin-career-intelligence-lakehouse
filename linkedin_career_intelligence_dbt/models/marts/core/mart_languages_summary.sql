select
    proficiency_track,
    count(*) as total_languages,
    count(distinct language_name_clean) as unique_languages
from {{ ref('int_languages_enriched') }}
group by
    proficiency_track
order by
    total_languages desc,
    proficiency_track asc