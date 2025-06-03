
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_teams
    

    
    as (select r.team_long, r.team, tournament_group, conf, alt_key, division
from NBA_MONTE_CARLO.PUBLIC.nba_raw_team_ratings r
    )
;


  