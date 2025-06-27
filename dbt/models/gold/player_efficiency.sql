with stats as (
    select * from {{ ref('player_stats') }}
)
select
    player_id,
    season,
    round(
        (
            (pts + reb + ast + stl + blk)
            - ((fga - fgm) + (fta - ftm) + turnover)
        ) / nullif(games_played, 0),
        3
    ) as efficiency
from stats
