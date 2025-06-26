{{ config(materialized='table') }}

select
    date_trunc('day', timestamp) as date,
    commodity,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price
from {{ ref('commodity_prices_bronze') }}
group by 1,2
