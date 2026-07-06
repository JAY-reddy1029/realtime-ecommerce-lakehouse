with user_orders as (
    select
        user_id,
        count(*)         as total_orders,
        sum(amount)       as total_spent,
        round(avg(amount), 2) as avg_order_value
    from {{ ref('stg_orders') }}
    group by user_id
),

user_sessions as (
    select
        user_id,
        count(*)                    as total_sessions,
        round(avg(duration_seconds), 2) as avg_session_duration,
        mode(device)                as most_used_device
    from {{ ref('stg_sessions') }}
    group by user_id
)

select
    coalesce(o.user_id, s.user_id) as user_id,
    coalesce(o.total_orders, 0)    as total_orders,
    coalesce(o.total_spent, 0)     as total_spent,
    o.avg_order_value,
    coalesce(s.total_sessions, 0)  as total_sessions,
    s.avg_session_duration,
    s.most_used_device
from user_orders o
full outer join user_sessions s
    on o.user_id = s.user_id