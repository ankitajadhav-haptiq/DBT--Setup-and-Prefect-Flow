-- Demo model added to verify the Agent PR Review pipeline end-to-end
-- (time/space complexity, quality, and clickable file links in the report).

with orders as (

    select * from {{ ref('raw_orders') }}

),

customers as (

    select * from {{ ref('raw_customers') }}

),

joined as (

    select
        o.order_id,
        o.order_date,
        c.customer_name,
        o.amount

    from orders o
    left join customers c
        on o.customer_id = c.customer_id

)

select
    order_id,
    order_date,
    customer_name,
    amount,
    row_number() over (
        partition by customer_name
        order by order_date
    ) as order_sequence

from joined
order by order_date desc
