
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_seasons
    

    
    as (select a.season from NBA_MONTE_CARLO.PUBLIC.nba_elo_history a group by all order by a.season
    )
;


  