import pandas as pd
import math
from snowflake.snowpark.functions import col


def calc_elo_diff(
    game_result: float,
    home_elo: float,
    visiting_elo: float,
    home_adv: float,
    scoring_margin: float,
) -> float:
    # just need to make sure i really get a game result that is float (annoying)
    game_result = float(game_result)
    adj_home_elo = float(home_elo) + float(home_adv)
    winner_elo_diff = (
        visiting_elo - adj_home_elo if game_result == 1 else adj_home_elo - visiting_elo
    )
    margin_of_victory_multiplier = math.log(abs(scoring_margin) + 1) * (
        2.2 / (winner_elo_diff * 0.001 + 2.2)
    )
    return (
        20.0
        * (
            (game_result)
            - (1.0 / (10.0 ** (-(visiting_elo - home_elo - home_adv) / 400.0) + 1.0))
        )
        * margin_of_victory_multiplier
    )


def model(dbt, sess):
    # get the existing elo ratings for the teams
    home_adv = dbt.config.get("nfl_elo_offset", 52.0)
    team_ratings = dbt.ref("nfl_raw_team_ratings")
    # Select only the columns you need
    ratings_df = team_ratings.select(col("team"), col("elo_rating"))

    # Collect results to the driver
    rows = ratings_df.collect()

    # Build the dictionary locally
    original_elo = {row["TEAM"]: float(row["ELO_RATING"]) for row in rows}
    working_elo = original_elo.copy()

    # loop over the historical game data and update the elo ratings as we go
    nba_elo_latest = (
        dbt.ref("nfl_latest_results")
        .select(
            col("game_id"),
            col("visiting_team"),
            col("home_team"),
            col("winning_team"),
            col("game_result"),
            col("neutral_site"),
            col("margin"),
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
        "MARGIN",
    ]
    rows = []
    for row in nba_rows:
        game_id = row["GAME_ID"]
        vteam = row["VISITING_TEAM"]
        hteam = row["HOME_TEAM"]
        winner = row["WINNING_TEAM"]
        game_result = row["GAME_RESULT"]
        neutral_site = row["NEUTRAL_SITE"]
        margin = row["MARGIN"]
        helo, velo = working_elo[hteam], working_elo[vteam]
        elo_change = calc_elo_diff(
            game_result, helo, velo, 0 if neutral_site == 1 else home_adv, margin
        )
        rows.append((game_id, vteam, velo, hteam, helo, winner, elo_change, margin))
        working_elo[hteam] -= elo_change
        working_elo[vteam] += elo_change

    return pd.DataFrame(columns=columns, data=rows)
