

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nba_reg_season_schedule
    HAVING COUNT(*) = 0

