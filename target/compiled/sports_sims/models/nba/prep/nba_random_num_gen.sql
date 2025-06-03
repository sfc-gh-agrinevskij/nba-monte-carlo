

WITH cte_scenario_gen AS (
    SELECT SEQ4() + 1 AS scenario_id
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
select
    i.scenario_id,
    s.game_id,
    CAST(RANDOM() * 10000 AS SMALLINT) AS rand_result,
    0 as sim_start_game_id
from cte_scenario_gen as i
cross join
    NBA_MONTE_CARLO.PUBLIC.nba_schedules as s
    -- LEFT JOIN NBA_MONTE_CARLO.PUBLIC.nba_latest_results AS R ON R.game_id = S.game_id
    -- WHERE R.game_id IS NULL OR (R.game_id IS NOT NULL AND i.scenario_id = 1)