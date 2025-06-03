
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.season_summary
    

    
    as (

select
    round(ratings.elo_rating, 0)::int
    || ' ('
    || case when original_rating < elo_rating then '+' else '' end
    || (elo_rating - original_rating)::int
    || ')' as elo_rating,
    r.*,
    p.made_playoffs,
    p.made_conf_semis,
    p.made_conf_finals,
    p.made_finals,
    p.won_finals
from NBA_MONTE_CARLO.PUBLIC.reg_season_summary r
left join NBA_MONTE_CARLO.PUBLIC.playoff_summary p on p.team = r.team
left join NBA_MONTE_CARLO.PUBLIC.nba_ratings ratings on ratings.team = r.team
    )
;


  