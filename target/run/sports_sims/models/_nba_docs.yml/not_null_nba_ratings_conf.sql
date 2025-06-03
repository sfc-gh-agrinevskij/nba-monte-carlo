
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select conf
from NBA_MONTE_CARLO.PUBLIC.nba_ratings
where conf is null



  
  
      
    ) dbt_internal_test