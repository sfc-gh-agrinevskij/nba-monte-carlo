select * from {{ ref("xf_series_to_seed") }} group by all
