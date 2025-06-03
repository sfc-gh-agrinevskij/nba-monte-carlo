

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nfl_random_num_gen
    HAVING COUNT(*) = 0

