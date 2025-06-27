select
    id as player_id,
    first_name,
    last_name,
    position,
    team_id
from {{ ref('stg_players') }}
