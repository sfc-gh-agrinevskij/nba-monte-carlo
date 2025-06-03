

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nba_raw_xf_series_to_seed
    HAVING COUNT(*) = 0

