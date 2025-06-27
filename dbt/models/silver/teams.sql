select
    id as team_id,
    full_name,
    abbreviation,
    city,
    conference,
    division
from {{ ref('stg_teams') }}
