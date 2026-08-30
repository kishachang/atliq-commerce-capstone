select
    product_id,
    product_name,
    supplier_name,
    supplier_cost,
    effective_date
from {{ source('silver', 'supplier_price_list') }}