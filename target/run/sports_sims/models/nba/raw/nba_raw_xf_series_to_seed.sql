
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_raw_xf_series_to_seed
    

    
    as (select * from NBA_MONTE_CARLO.SEED.xf_series_to_seed group by all
    )
;


  