-- Demo model #2 to verify the Agent PR Review pipeline
-- (covers a different mix of findings than test_pr_demo.sql did).

with hours_by_site as (

    select * from {{ ref('raw_hours') }}

),

sizes_by_site as (

    select * from {{ ref('raw_sizes') }}

)

select *
from hours_by_site ah
cross join sizes_by_site ls
where upper(trim(ah.site_id)) = upper(trim(ls.site_id))
  and ah.usage_date = ls.usage_date
