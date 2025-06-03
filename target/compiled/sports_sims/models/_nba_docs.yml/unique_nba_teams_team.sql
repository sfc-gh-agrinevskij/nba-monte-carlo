
    
    

select
    team as unique_field,
    count(*) as n_records

from NBA_MONTE_CARLO.PUBLIC.nba_teams
where team is not null
group by team
having count(*) > 1


