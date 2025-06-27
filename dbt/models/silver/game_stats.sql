select
    game_id,
    player_id,
    team_id,
    pts,
    reb,
    ast
from {{ ref('stg_game_stats') }}
