
  
    

create or replace transient table NBA_MONTE_CARLO.PUBLIC.nfl_reg_season_simulator
    

    
    as (select
    r.scenario_id,
    s.*,
    ( 1 - (1 / (POWER(10, (-( S.visiting_team_elo_rating - S.home_team_elo_rating - 52)::real/400)+1)))) * 10000 as home_team_win_probability,
    r.rand_result,
    case
        when lr.include_actuals = 'true'
            then lr.winning_team
        when
            (( 1 - (1 / (POWER(10, (-( S.visiting_team_elo_rating - S.home_team_elo_rating - 52)::real/400)+1)))) * 10000)::int >= r.rand_result
        then s.home_team
        else s.visiting_team
    end as winning_team,
    coalesce(lr.include_actuals, false) as include_actuals
from NBA_MONTE_CARLO.PUBLIC.nfl_schedules s
left join NBA_MONTE_CARLO.PUBLIC.nfl_random_num_gen r on r.game_id = s.game_id
left join NBA_MONTE_CARLO.PUBLIC.nfl_latest_results lr on lr.game_id = s.game_id
where s.type = 'reg_season'
    )
;


  