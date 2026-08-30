select
    customer_id,
    customer_name,
    email,
    city,
    signup_date,
    updated_at
from {{ source('silver', 'customers') }}