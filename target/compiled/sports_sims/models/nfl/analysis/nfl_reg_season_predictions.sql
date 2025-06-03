select
    game_id,
    week_number,
    type,
    home_team,
    home.team_short as home_short,
    home_team_elo_rating,
    visiting_team,
    visitor.team_short as vis_short,
    visiting_team_elo_rating,
    home_team_elo_rating - visiting_team_elo_rating as elo_diff,
    home_team_win_probability,
    winning_team,
    include_actuals,
    count(*) as occurances,
    CASE WHEN home_team_win_probability/10000 >= 0.5 
        THEN '-' || ROUND( home_team_win_probability/10000 / ( 1.0 - home_team_win_probability/10000 ) * 100 )::int
        ELSE '+' || ((( 1.0 - home_team_win_probability/10000 ) / (home_team_win_probability/10000::real ) * 100)::int)
    END as american_odds
from NBA_MONTE_CARLO.PUBLIC.nfl_reg_season_simulator s
left join NBA_MONTE_CARLO.PUBLIC.nfl_ratings home on home.team = s.home_team
left join NBA_MONTE_CARLO.PUBLIC.nfl_ratings visitor on visitor.team = s.visiting_team
group by all