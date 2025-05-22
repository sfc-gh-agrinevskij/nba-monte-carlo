{{ config(materialized="table") }}

with
    cte_games as (
        select
            team1,
            team2,
            score1,
            score2,
            playoff,
            case when score1 > score2 then team1 else team2 end as winner,
            case when score1 < score2 then team1 else team2 end as loser,
            case when team1 = t.team then elo1_pre else elo2_pre end as elo,
            case when team1 = t.team then score1 else score2 end as pf,
            case when team1 = t.team then score2 else score1 end as pa,
            t.team || ':' || t.season as key,
            t.team,
            t.season
        from {{ ref("nba_elo_history") }} h
        left join
            {{ ref("nba_season_teams") }} t
            on (t.team = h.team1 or t.team = h.team2)
            and h.season = t.season
    )
select
    key,
    count(*) as ct,
    SUM(CASE WHEN winner = team AND playoff = 'r' THEN 1 ELSE 0 END) AS wins,
    -SUM(CASE WHEN loser = team AND playoff = 'r' THEN 1 ELSE 0 END) AS losses,
     SUM(CASE WHEN winner = team AND team1 = team AND playoff = 'r' THEN 1 ELSE 0 END) AS home_wins,
    -SUM(CASE WHEN loser = team AND team1 = team AND playoff = 'r' THEN 1 ELSE 0 END) AS home_losses,
    SUM(CASE WHEN winner = team AND team2 = team AND playoff = 'r' THEN 1 ELSE 0 END) AS away_wins,
    -SUM(CASE WHEN loser = team AND team2 = team AND playoff = 'r' THEN 1 ELSE 0 END) AS away_losses,
    SUM(CASE WHEN winner = team AND playoff <> 'r' THEN 1 ELSE 0 END) AS playoff_wins,
    -SUM(CASE WHEN loser = team AND playoff <> 'r' THEN 1 ELSE 0 END) AS playoff_losses,

    avg(pf) as pf,
    avg(- pa) as pa,
    avg(pf) - avg(pa) as margin,
    min(elo) as min_elo,
    avg(elo) as avg_elo,
    max(elo) as max_elo,
    team as team,
    season as season
from cte_games
group by all
