select
    product_id,
    count(*)                                        as total_orders,
    sum(amount)                                      as total_revenue,
    round(avg(amount), 2)                            as avg_order_value,
    sum(case when status = 'delivered' then 1 else 0 end)  as delivered_count,
    sum(case when status = 'cancelled' then 1 else 0 end)  as cancelled_count,
    round(
        100.0 * sum(case when status = 'cancelled' then 1 else 0 end) / count(*),
        2
    ) as cancellation_rate_pct
from {{ ref('stg_orders') }}
group by product_id