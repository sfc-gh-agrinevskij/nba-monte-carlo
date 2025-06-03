select
    week as wk,
    Winner_tie as winner,
    ptsw as winner_pts,
    Loser_tie as loser,
    ptsl as loser_pts,
    case when ptsl = ptsw then 1 else 0 end as tie_flag
from NBA_MONTE_CARLO.PUBLIC.nfl_results