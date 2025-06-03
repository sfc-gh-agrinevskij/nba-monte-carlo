

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nfl_raw_results
    HAVING COUNT(*) = 0

