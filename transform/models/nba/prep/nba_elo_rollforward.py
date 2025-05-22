import pandas as pd
from snowflake.snowpark.functions import col


def calc_elo_diff(
    margin: float,
    game_result: float,
    home_elo: float,
    visiting_elo: float,
    home_adv: float,
) -> float:
    # just need to make sure i really get a game result that is float (annoying)
    game_result = float(game_result)
    elo_diff = -float((visiting_elo - home_elo - home_adv))
    raw_elo = 20.0 * ((game_result) - (1.0 / (10.0 ** (elo_diff / 400.0) + 1.0)))
    if game_result == 1:
        elo_chg = raw_elo * ((margin + 3) ** 0.8) / (7.5 + (0.006 * elo_diff))
    elif game_result == 0:
        elo_chg = raw_elo * ((margin + 3) ** 0.8) / (7.5 + (0.006 * -elo_diff))
    return elo_chg


def model(dbt, sess):
    # get the existing elo ratings for the teams
    home_adv = dbt.config.get("nba_elo_offset", 100.0)
    team_ratings = dbt.ref("nba_raw_team_ratings")
    ratings_df = team_ratings.select(col("team_long"), col("elo_rating"))
    rows = ratings_df.collect()
    original_elo = {row["TEAM_LONG"]: float(row["ELO_RATING"]) for row in rows}
    working_elo = original_elo.copy()

    # loop over the historical game data and update the elo ratings as we go
    nba_elo_latest = (
        dbt.ref("nba_latest_results")
        .select(
            col("game_id"),
            col("visiting_team"),
            col("home_team"),
            col("winning_team"),
            col("margin"),
            col("game_result"),
        )
        .sort(col("game_id"))
    )
    nba_rows = nba_elo_latest.collect()
    columns = [
        "GAME_ID",
        "VISITING_TEAM",
        "VISITING_TEAM_ELO_RATING",
        "HOME_TEAM",
        "HOME_TEAM_ELO_RATING",
        "WINNING_TEAM",
        "ELO_CHANGE",
    ]
    rows = []
    for row in nba_rows:
        game_id = row["GAME_ID"]
        vteam = row["VISITING_TEAM"]
        hteam = row["HOME_TEAM"]
        winner = row["WINNING_TEAM"]
        margin = row["MARGIN"]
        game_result = row["GAME_RESULT"]
        helo, velo = working_elo[hteam], working_elo[vteam]
        elo_change = calc_elo_diff(margin, game_result, helo, velo, home_adv)
        rows.append((game_id, vteam, velo, hteam, helo, winner, elo_change))
        working_elo[hteam] -= elo_change
        working_elo[vteam] += elo_change

    return pd.DataFrame(columns=columns, data=rows)
