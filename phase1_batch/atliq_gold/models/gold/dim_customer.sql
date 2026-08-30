select
    customer_id,
    customer_name,
    city,
    signup_date,

    cast(
        date_trunc('month', signup_date)
        as date
    ) as signup_cohort

from {{ ref('stg_customers') }}