select
    event_id,
    product_id,
    warehouse_id,
    quantity,
    action,
    timestamp as inventory_timestamp,
    processed_timestamp
from delta_scan('../../data/silver/inventory')