select
    order_id,
    customer_id,
    order_date,
    status,
    order_amount,
    created_at,
    updated_at
from {{ source('silver', 'orders') }}