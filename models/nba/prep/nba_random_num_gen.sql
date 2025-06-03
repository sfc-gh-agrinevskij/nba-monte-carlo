{{ config(materialized="table") }}

WITH cte_scenario_gen AS (
    SELECT SEQ4() + 1 AS scenario_id
    FROM TABLE(GENERATOR(ROWCOUNT => {{ var("scenarios") }}))
)
select
    i.scenario_id,
    s.game_id,
    CAST(RANDOM() * 10000 AS SMALLINT) AS rand_result,
    {{ var("sim_start_game_id") }} as sim_start_game_id
from cte_scenario_gen as i
cross join
    {{ ref("nba_schedules") }} as s
    -- LEFT JOIN {{ ref( 'nba_latest_results' )}} AS R ON R.game_id = S.game_id
    -- WHERE R.game_id IS NULL OR (R.game_id IS NOT NULL AND i.scenario_id = 1)
    
