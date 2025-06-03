
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nba_raw_schedule
    

    
    as (select
    id,
    type,
    TO_DATE(Year || ' ' || Date, 'YYYY MON DD')::date as date,
    Start_ET as "Start (ET)",
    Visitor_Neutral as VisTm,
    Home_Neutral as HomeTm,
    Attend as "Attend.",
    arena,
    notes,
    series_id
from NBA_MONTE_CARLO.SEED.nba_schedule
where arena is null -- make sure playoffs are included
    or arena <> 'Placeholder' -- removing IST games w/o teams & arena
    )
;


  