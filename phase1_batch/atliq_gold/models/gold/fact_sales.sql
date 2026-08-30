select
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.quantity,
    oi.item_price,

    cast(
        oi.quantity * oi.item_price
        as decimal(12,2)
    ) as gross_revenue,

    o.status

from {{ ref('stg_order_items') }} oi

inner join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id