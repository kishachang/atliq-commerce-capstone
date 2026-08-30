select
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    s.supplier_name,
    s.supplier_cost,

    cast(
        p.unit_price - s.supplier_cost
        as decimal(12,2)
    ) as unit_margin

from {{ ref('stg_products') }} p

left join {{ ref('stg_supplier_price_list') }} s
    on p.product_id = s.product_id