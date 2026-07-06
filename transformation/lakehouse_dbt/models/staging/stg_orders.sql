select
    order_id,
    user_id,
    product_id,
    amount,
    status,
    timestamp as order_timestamp,
    processed_timestamp
from delta_scan('../../data/silver/orders')