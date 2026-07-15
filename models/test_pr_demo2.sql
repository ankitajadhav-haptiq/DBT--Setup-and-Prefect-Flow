-- Demo model #2 to verify the Agent PR Review pipeline
-- (covers a different mix of findings than test_pr_demo.sql did).

with hours_by_site as (

    select
        site_id,
        usage_date,
        total_hours
    from {{ ref('raw_hours') }}

),

sizes_by_site as (

    select
        site_id,
        usage_date,
        size_gb
    from {{ ref('raw_sizes') }}

)

select
    ah.site_id,
    ah.usage_date,
    ah.total_hours,
    ls.size_gb
from hours_by_site as ah
inner join sizes_by_site as ls
    on upper(trim(ah.site_id)) = upper(trim(ls.site_id))
    and ah.usage_date = ls.usage_date
