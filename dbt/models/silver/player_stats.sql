select
    player_id,
    season,
    games_played,
    pts,
    reb,
    ast,
    stl,
    blk,
    turnover,
    fga,
    fgm,
    fta,
    ftm
from {{ ref('stg_player_stats') }}
