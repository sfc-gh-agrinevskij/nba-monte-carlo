

    SELECT COALESCE(COUNT(*),0) AS records
    FROM NBA_MONTE_CARLO.PUBLIC.nba_raw_team_ratings
    HAVING COUNT(*) = 0

