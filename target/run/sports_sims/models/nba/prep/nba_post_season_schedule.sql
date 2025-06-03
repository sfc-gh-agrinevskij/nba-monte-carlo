
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_post_season_schedule
    

    
    as (select
    s.id::int as game_id,
    s.date,
    s.type,
    s.series_id,
    null as visiting_conf,
    s.vistm as visiting_team,
    null as visiting_team_elo_rating,
    null as home_conf,
    s.hometm as home_team,
    null as home_team_elo_rating
from NBA_MONTE_CARLO.PUBLIC.nba_raw_schedule as s
-- LEFT JOIN NBA_MONTE_CARLO.PUBLIC.nba_ratings V ON V.team = S.VisTm
-- LEFT JOIN NBA_MONTE_CARLO.PUBLIC.nba_ratings H ON H.team = S.HomeTm
-- LEFT JOIN NBA_MONTE_CARLO.PUBLIC.nba_elo_rollforward R ON R.game_id = S.id
where s.type <> 'reg_season'
group by all
    )
;


  