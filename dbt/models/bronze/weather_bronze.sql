{{ config(materialized='table') }}
select
    cast(timestamp as timestamp) as timestamp,
    temperature_c
from {{ ref('weather') }}
