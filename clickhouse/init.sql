create table if not exists orders_aggregated(
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    customer_city String,
    customer_state String
) Engine = MergeTree()
order by (order_purchase_timestamp, order_id);