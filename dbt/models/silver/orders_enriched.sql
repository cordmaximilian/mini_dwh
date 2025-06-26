{{ config(materialized='table') }}

select
    o.order_id,
    o.user_id,
    o.amount,
    cast(o.order_date as date) as order_date,
    cast(u.signup_date as date) as signup_date,
    u.country
from {{ ref('orders_bronze') }} as o
left join {{ ref('users_bronze') }} as u
  on o.user_id = u.user_id
