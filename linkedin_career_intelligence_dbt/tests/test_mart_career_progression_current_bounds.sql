select *
from {{ ref('mart_career_progression') }}
where current_positions_started > total_positions_started
