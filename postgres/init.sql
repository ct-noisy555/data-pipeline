create table if not exists customers (
    customer_id varchar(8) bigseral primary key,
    customer_unique_id varchar(8),
    customer_zip_code_prefix varchar(10),
    customer_city varchar(50),
    customer_state varchar(5)
);

create table if not exists orders (
    order_id varchar(8) bigserial primary key,
    customer_id varchar(8),
    order_status varchar(15),
    order_purchase_timestamp timestamp,
    order_approved_ad timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
);

copy customers from '/data/olist_customers_dataset.csv' with (format csv, HEADER true);
copy orders from '/data/olist_orders_dataset.csv' WITH (format csv, header true);