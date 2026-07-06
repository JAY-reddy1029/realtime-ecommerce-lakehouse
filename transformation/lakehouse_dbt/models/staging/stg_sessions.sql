select
    session_id,
    user_id,
    page_viewed,
    duration_seconds,
    device,
    timestamp as session_timestamp,
    processed_timestamp
from delta_scan('../../data/silver/sessions')