
  create or replace   view NBA_MONTE_CARLO.PUBLIC.nfl_vegas_wins
  
   as (
    select team, win_total from NBA_MONTE_CARLO.PUBLIC.nfl_ratings group by all
  );

