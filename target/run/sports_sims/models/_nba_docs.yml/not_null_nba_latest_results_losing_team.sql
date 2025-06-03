
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select losing_team
from NBA_MONTE_CARLO.PUBLIC.nba_latest_results
where losing_team is null



  
  
      
    ) dbt_internal_test