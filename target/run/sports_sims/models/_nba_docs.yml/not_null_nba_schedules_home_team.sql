
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select home_team
from NBA_MONTE_CARLO.PUBLIC.nba_schedules
where home_team is null



  
  
      
    ) dbt_internal_test