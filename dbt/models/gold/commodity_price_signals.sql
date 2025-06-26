{{ config(materialized='table') }}

with daily as (
    select * from {{ ref('commodity_prices_daily') }}
),
calc as (
    select
        date,
        commodity,
        avg_price,
        avg(avg_price) over (
            partition by commodity
            order by date
            rows between 6 preceding and current row
        ) as avg_price_7d
    from daily
)
select
    date,
    commodity,
    avg_price,
    avg_price_7d,
    avg_price < avg_price_7d as is_buy_signal
from calc
order by commodity, date
