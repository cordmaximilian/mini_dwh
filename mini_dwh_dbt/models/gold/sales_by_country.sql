{{ config(materialized='table') }}

select
    country,
    sum(amount) as total_sales,
    count(order_id) as order_count
from {{ ref('orders_enriched') }}
group by country
