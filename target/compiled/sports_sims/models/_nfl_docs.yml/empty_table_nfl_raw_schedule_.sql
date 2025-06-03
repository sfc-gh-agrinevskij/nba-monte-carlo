

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nfl_raw_schedule
    HAVING COUNT(*) = 0

