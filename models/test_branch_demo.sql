-- Test model added on test_branch to verify branch + push workflow.

with source_data as (

    select
        site_id,
        usage_date,
        total_hours
    from {{ ref('raw_hours') }}

)

select
    site_id,
    usage_date,
    total_hours
from source_data
