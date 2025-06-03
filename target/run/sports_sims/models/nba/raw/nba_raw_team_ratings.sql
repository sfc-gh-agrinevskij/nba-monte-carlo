
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_raw_team_ratings
    

    
    as (select * from NBA_MONTE_CARLO.SEED.nba_team_ratings
    )
;


  