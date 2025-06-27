with stocks as (
    select * from {{ ref('stock_prices') }}
),
commodities as (
    select * from {{ ref('commodity_prices') }}
),
weather as (
    select * from {{ ref('weather') }}
),
news as (
    select * from {{ ref('news') }}
)
select
    s.date,
    s.ticker as stock_ticker,
    s.adj_close as stock_close,
    c.ticker as commodity_ticker,
    c.adj_close as commodity_close,
    w.tavg,
    n.title as headline
from stocks s
left join commodities c on s.date = c.date
left join weather w on s.date = w.date
left join news n on s.date = n.date
