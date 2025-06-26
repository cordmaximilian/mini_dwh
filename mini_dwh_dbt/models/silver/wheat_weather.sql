{{ config(materialized='table') }}

select
    cp.timestamp,
    cp.price as wheat_price,
    w.temperature_c
from {{ ref('commodity_prices_bronze') }} as cp
left join {{ ref('weather_bronze') }} as w
  on cp.timestamp = w.timestamp
where cp.commodity = 'wheat'
