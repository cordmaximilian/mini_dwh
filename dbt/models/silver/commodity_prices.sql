select
    cast(date as date) as date,
    ticker,
    "Adj Close" as adj_close
from {{ ref('stg_commodity_prices') }}
