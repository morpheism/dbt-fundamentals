with payments as (
    select * from {{ ref('stg_payments') }}
), orders as (
    select * from {{ ref('stg_orders') }}
), order_payments as (
    select
        order_id,
        sum(case when payment_status = 'success' then amount end) as amount
    from payments
    group by 1
), final as (
    select
        p.order_id,
        o.customer_id,
        p.amount
    from order_payments as p
    inner join orders as o
        on p.order_id = o.order_id
)
select * from final
