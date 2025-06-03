
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.initialize_seeding
    

    
    as (with
    cte_teams as (
        select scenario_id, conf, winning_team, seed, elo_rating
        from NBA_MONTE_CARLO.PUBLIC.reg_season_end
        where season_rank < 7
        union all
        select *
        from NBA_MONTE_CARLO.PUBLIC.playin_sim_r2_end
    )

select t.*, 0 as sim_start_game_id
from cte_teams t
    )
;


  