
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        conf as value_field,
        count(*) as n_records

    from NBA_MONTE_CARLO.PUBLIC.nfl_ratings
    group by conf

)

select *
from all_values
where value_field not in (
    'AFC','NFC'
)



  
  
      
    ) dbt_internal_test