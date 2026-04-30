select trim(skill_name) as skill_name
from {{ source('bronze', 'skills') }}
