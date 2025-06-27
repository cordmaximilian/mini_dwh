select
    cast(date as date) as date,
    title,
    url
from {{ ref('stg_news') }}
