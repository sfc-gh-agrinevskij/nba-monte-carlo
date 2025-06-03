
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_vegas_wins
    

    
    as (select team, win_total::double as win_total from NBA_MONTE_CARLO.PUBLIC.nba_ratings group by all
    )
;


  