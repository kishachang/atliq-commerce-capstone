select
    payment_id,
    order_id,
    amount,
    method,
    paid_at,
    updated_at
from {{ source('silver', 'payments') }}