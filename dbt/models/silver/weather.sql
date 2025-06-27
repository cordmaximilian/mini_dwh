select
    cast(time as date) as date,
    tavg
from {{ ref('stg_weather') }}
