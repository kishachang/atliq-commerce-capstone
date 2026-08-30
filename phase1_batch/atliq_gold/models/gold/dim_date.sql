with date_spine as (

    select explode(
        sequence(
            to_date('2024-01-01'),
            to_date('2026-12-31'),
            interval 1 day
        )
    ) as date_day

)

select
    date_day,
    day(date_day) as day_of_month,
    month(date_day) as month_number,
    date_format(date_day, 'MMMM') as month_name,
    quarter(date_day) as quarter_number,
    year(date_day) as year_number,
    date_format(date_day, 'EEEE') as weekday_name,
    dayofweek(date_day) as weekday_number,

    cast(
        date_trunc('month', date_day)
        as date
    ) as month_start

from date_spine