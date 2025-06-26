{{ config(materialized='table') }}
select
    cast(timestamp as timestamp) as timestamp,
    price,
    commodity
from {{ ref('commodity_prices') }}
