
with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('fct_orders') }}
),
order_date as (
    select
        order_id,
        order_date
    from {{ ref('stg_orders') }}
),
customer_orders as (

    select
        o.customer_id,

        min(d.order_date) as first_order_date,
        max(d.order_date) as most_recent_order_date,
        count(o.order_id) as number_of_orders,
        sum(o.amount) as lifetime_value

    from orders as o
    inner join order_date d
        on o.order_id = d.order_id

    group by 1

),
final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.lifetime_value

    from customers

    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

)

select * from final