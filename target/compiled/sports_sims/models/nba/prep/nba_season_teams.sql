select c.*
from
    (
        select a.season, a.team1 as team
        from NBA_MONTE_CARLO.PUBLIC.nba_elo_history a
        union all
        select b.season, b.team2
        from NBA_MONTE_CARLO.PUBLIC.nba_elo_history b
    ) as c
group by all
order by c.team