select
    id as game_id,
    date,
    season,
    home_team_id,
    visitor_team_id,
    home_team_score,
    visitor_team_score,
    postseason
from {{ ref('stg_games') }}
