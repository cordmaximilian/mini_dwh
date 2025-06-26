{{ config(materialized='table') }}

with signals as (
    select * from {{ ref('commodity_price_signals') }}
),
weather as (
    select * from {{ ref('daily_weather') }}
)
select
    s.date,
    s.commodity,
    s.avg_price,
    s.avg_price_7d,
    s.is_buy_signal and w.avg_temperature_c > 5 as favorable_buy
from signals s
left join weather w on s.date = w.date
where s.commodity in ('wheat', 'corn', 'soybeans', 'oil', 'fertilizer')
order by s.date, s.commodity
