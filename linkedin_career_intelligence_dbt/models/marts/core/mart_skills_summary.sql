select
    skill_category,
    count(*) as total_skills,
    count(distinct skill_name_clean) as unique_skills,
    round(avg(skill_name_length), 2) as avg_skill_name_length
from {{ ref('int_skills_enriched') }}
group by skill_category
order by total_skills desc, skill_category
