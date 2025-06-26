{{ config(materialized='table') }}

select
    cp.timestamp,
    cp.price as wheat_price,
    w.temperature_c
from {{ ref('commodity_prices') }} as cp
left join {{ ref('weather') }} as w
  on cp.timestamp = w.timestamp
where cp.commodity = 'wheat'
