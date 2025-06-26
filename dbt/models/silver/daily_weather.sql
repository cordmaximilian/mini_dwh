{{ config(materialized='table') }}

select
    date_trunc('day', timestamp) as date,
    avg(temperature_c) as avg_temperature_c
from {{ ref('weather_bronze') }}
group by 1
