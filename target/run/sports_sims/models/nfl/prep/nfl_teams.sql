
  create or replace   view NBA_MONTE_CARLO.PUBLIC.nfl_teams
  
   as (
    select s.vistm as team_long,
-- R.team
from NBA_MONTE_CARLO.PUBLIC.nfl_raw_schedule s
-- LEFT JOIN NBA_MONTE_CARLO.PUBLIC.nfl_ratings AS R ON R.team = S.VisTm
-- WHERE R.team IS NOT NULL
group by all
  );

