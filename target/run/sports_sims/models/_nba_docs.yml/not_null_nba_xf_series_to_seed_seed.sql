
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seed
from NBA_MONTE_CARLO.PUBLIC.nba_xf_series_to_seed
where seed is null



  
  
      
    ) dbt_internal_test