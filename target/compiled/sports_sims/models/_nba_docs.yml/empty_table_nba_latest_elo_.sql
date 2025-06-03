

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nba_latest_elo
    HAVING COUNT(*) = 0

