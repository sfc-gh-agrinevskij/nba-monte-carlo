
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_schedules
    

    
    as (select *
from NBA_MONTE_CARLO.PUBLIC.nba_reg_season_schedule
union all
select *
from NBA_MONTE_CARLO.PUBLIC.nba_post_season_schedule
    )
;


  