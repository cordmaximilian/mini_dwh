with stats as (
    select * from {{ ref('game_stats') }}
),
players as (
    select * from {{ ref('players') }}
),
teams as (
    select * from {{ ref('teams') }}
),
games as (
    select * from {{ ref('games') }}
)
select
    stats.game_id,
    games.date,
    games.season,
    stats.player_id,
    players.first_name,
    players.last_name,
    players.position,
    stats.team_id,
    teams.full_name as team_name,
    games.home_team_id,
    games.visitor_team_id,
    games.home_team_score,
    games.visitor_team_score,
    stats.pts,
    stats.reb,
    stats.ast
from stats
left join players on stats.player_id = players.player_id
left join teams on stats.team_id = teams.team_id
left join games on stats.game_id = games.game_id
