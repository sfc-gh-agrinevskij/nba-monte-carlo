
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_xf_series_to_seed
    

    
    as (select series_id, seed from NBA_MONTE_CARLO.PUBLIC.nba_raw_xf_series_to_seed
    )
;


  