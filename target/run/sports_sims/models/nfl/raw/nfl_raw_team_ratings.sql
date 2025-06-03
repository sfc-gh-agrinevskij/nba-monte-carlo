
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nfl_raw_team_ratings
    

    
    as (select
    Team as team,
    Team_short as team_short,
    Win_Total as win_total,
    ELO_rating as elo_rating,
    Conf as conf,
    Division as division
from NBA_MONTE_CARLO.PUBLIC.nfl_team_ratings
    )
;


  