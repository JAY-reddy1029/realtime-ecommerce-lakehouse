select
    o.order_id,
    o.user_id,
    o.product_id,
    o.amount,
    o.status,
    o.order_timestamp,
    date_trunc('day', o.order_timestamp)   as order_date,
    date_trunc('month', o.order_timestamp) as order_month,
    extract(hour from o.order_timestamp)   as order_hour,
    extract(dow from o.order_timestamp)    as order_day_of_week,
    case
        when o.status = 'delivered' then true
        else false
    end as is_completed,
    case
        when o.status = 'cancelled' then true
        else false
    end as is_cancelled
from {{ ref('stg_orders') }} o