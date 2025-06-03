select
    Team as team,
    Team_short as team_short,
    Win_Total as win_total,
    ELO_rating as elo_rating,
    Conf as conf,
    Division as division
from {{ ref("nfl_team_ratings") }}
