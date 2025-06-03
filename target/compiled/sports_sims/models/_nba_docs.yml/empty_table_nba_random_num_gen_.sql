

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nba_random_num_gen
    HAVING COUNT(*) = 0

