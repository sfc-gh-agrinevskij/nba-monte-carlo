with

    -- Replace this with your dbt-generated or materialized sequence/numbers table
    scenario_ids as (
        select seq4() + 1 as scenario_id
        from table(generator(rowcount => {{ var("scenarios") }}))
    ),

    cte_wins as (
        select
            s.scenario_id,
            s.winning_team,
            case
                when s.winning_team = s.home_team then s.home_conf else s.visiting_conf
            end as conf,
            count(*) as wins,
            sum(case when include_actuals = true then 1 else 0 end) as actual_wins
        from {{ ref("reg_season_simulator") }} s
        where s.type = 'tournament'
        group by s.scenario_id, s.winning_team, conf
    ),

    cte_losses as (
        select
            s.scenario_id,
            case
                when s.home_team = s.winning_team then s.visiting_team else s.home_team
            end as losing_team,
            case
                when s.winning_team = s.home_team then s.visiting_conf else s.home_conf
            end as conf,
            count(*) as losses,
            sum(case when include_actuals = true then 1 else 0 end) as actual_losses
        from {{ ref("reg_season_simulator") }} s
        where s.type = 'tournament'
        group by s.scenario_id, losing_team, conf
    ),

    cte_results_with_group as (
        select
            scenarios.scenario_id,
            t.team as winning_team,
            t.conf,
            coalesce(w.wins, 0) as wins,
            coalesce(l.losses, 0) as losses,
            t.tournament_group,
            coalesce(w.actual_wins, 0) as actual_wins,
            coalesce(l.actual_losses, 0) as actual_losses
        from {{ ref("nba_teams") }} t
        cross join scenario_ids scenarios
        left join cte_wins w
            on t.team = w.winning_team and scenarios.scenario_id = w.scenario_id
        left join cte_losses l
            on t.team = l.losing_team and scenarios.scenario_id = l.scenario_id
    ),

    cte_home_margin as (
        select
            t.team,
            coalesce(sum(coalesce(-h.actual_margin, -h.implied_line)), 0) as home_pt_diff
        from {{ ref("nba_teams") }} t
        left join {{ ref("reg_season_predictions") }} h
            on h.home_team = t.team
            and h.type = 'tournament'
            and (
                (h.actual_margin is null and h.winning_team = h.home_team)
                or (h.actual_margin is not null)
            )
        group by t.team
    ),

    cte_visitor_margin as (
        select
            t.team,
            coalesce(sum(coalesce(v.actual_margin, v.implied_line)), 0) as visitor_pt_diff
        from {{ ref("nba_teams") }} t
        left join {{ ref("reg_season_predictions") }} v
            on v.visiting_team = t.team
            and v.type = 'tournament'
            and (
                (v.actual_margin is null and v.winning_team = v.home_team)
                or (v.actual_margin is not null)
            )
        group by t.team
    ),

    cte_head_to_head as (
        select
            g.scenario_id,
            g.winning_team,
            case
                when g.winning_team = g.home_team then g.visiting_team else g.home_team
            end as losing_team
        from {{ ref("reg_season_simulator") }} g
        where g.type = 'tournament'
    ),

    cte_head_to_head_wins as (
        select h.scenario_id, h.winning_team as team, count(*) as h2h_wins
        from cte_head_to_head h
        join cte_wins w
            on h.winning_team = w.winning_team
            and h.scenario_id = w.scenario_id
            and h.losing_team in (
                select winning_team
                from cte_wins
                where
                    wins = w.wins
                    and winning_team != w.winning_team
                    and scenario_id = w.scenario_id
            )
        group by h.scenario_id, h.winning_team
    ),

    cte_fuzz as (
        select
            r.scenario_id,
            r.winning_team,
            ((r.wins - r.actual_wins) * floor(uniform(0, 5, random())))
            + ((r.losses - r.actual_losses) * floor(uniform(-5, 0, random()))) as fuzz
        from cte_results_with_group r
    ),

    cte_ranked_wins as (
        select
            r.*,
            h2h.h2h_wins,
            coalesce(h.home_pt_diff, 0) + coalesce(v.visitor_pt_diff, 0) + f.fuzz as pt_diff,
            row_number() over (
                partition by r.scenario_id, tournament_group
                order by wins desc, h2h.h2h_wins desc, pt_diff desc
            ) as group_rank
        from cte_results_with_group r
        left join cte_home_margin h on h.team = r.winning_team
        left join cte_visitor_margin v on v.team = r.winning_team
        left join cte_head_to_head_wins h2h on h2h.team = r.winning_team and h2h.scenario_id = r.scenario_id
        left join cte_fuzz f on f.scenario_id = r.scenario_id and f.winning_team = r.winning_team
    ),

    cte_wildcard as (
        select
            scenario_id,
            winning_team,
            conf,
            wins,
            pt_diff,
            group_rank,
            row_number() over (
                partition by scenario_id, conf
                order by wins desc, pt_diff desc, uniform(0, 1, random())
            ) as wildcard_rank
        from cte_ranked_wins
        where group_rank = 2
    ),

    cte_made_tournament as (
        select
            w.*,
            case when w.group_rank = 1 then 1 else 0 end as made_tournament,
            case when wc.wildcard_rank = 1 then 1 else 0 end as made_wildcard,
            w.tournament_group || '-' || cast(w.group_rank as string) as seed
        from cte_ranked_wins w
        left join cte_wildcard wc
            on wc.winning_team = w.winning_team and wc.scenario_id = w.scenario_id
    )

select
    mp.*,
    le.elo_rating,
    {{ var("sim_start_game_id") }} as sim_start_game_id
from cte_made_tournament mp
left join {{ ref("nba_latest_elo") }} le on le.team = mp.winning_team