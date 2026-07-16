-- Demo model for the Agent PR Review presentation.
-- Deliberately includes a range of patterns so the automated review
-- can showcase security, complexity, and quality checks side by side.

with revenue_raw as (

    select * from {{ ref('raw_revenue') }}

),

customers_raw as (

    select * from {{ ref('raw_customers') }}

)

select *
from revenue_raw r
cross join customers_raw c
where upper(trim(r.customer_id)) = upper(trim(c.customer_id))
  and upper(trim(r.region)) = upper(trim(c.region))
  and upper(trim(r.channel)) = upper(trim(c.channel))
qualify row_number() over (
    partition by r.customer_id
    order by r.revenue_date desc
) = 1
order by r.revenue_date desc
