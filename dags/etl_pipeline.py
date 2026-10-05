from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kafka import KafkaProducer, KafkaConsumer
from clickhouse_driver import Client
import pandas as pd
import json

default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2026, 1, 1),
}

def send_aggregated_to_kafka():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    query="""
        SELECT
            o.order_id,
            o.customer_id,
            o.order_status,
            0.order_purchase_timestamp,
            c.customer_city,
            c.customer_state
        from orders o
        join customers c on o.customer_id=c.customer_id
        limit 1000;
    """
    df = pg_hook.get_pandas_df(query)

    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    for _, row in df.iterrows():
        producer.send('orders_aggregated_topic', row.to_dict())
    producer.flush()
    print(f"sent {len(df)} rows to Kafka")

def consume_kafka_load_clickhouse():
    consumer = KafkaConsumer(
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=15000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    client = Client(host='clickhouse', port=9000)
    batch=[]

    for message in consumer:
        r = message.value
        batch.append((
            r['order_id'],
            r['customer_id'],
            r['order_status'],
            r['order_purchase_timestamp'],
            r['costumer_city'],
            r['customer_state']
        ))

    if batch:
        client.execute(
            batch
        )
        print(f"Loaded {len(batch)} rows to Clickhouse")
    else:
        print("No messages consumed from Kafka")

with DAG(
    'data-pipeline',
    default_args=default_args,
    description='ETL: Postgres JOIN -> Kafka -> Clickhouse',
    schedule_interval = None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id = 'send_aggregated_to_kafka',
        python_callable = send_aggregated_to_kafka,
    )

    t2 = PythonOperator(
        task_id='consume_kafka_load_clickhouse',
        python_callable = consume_kafka_load_clickhouse,
    )

    t1 >> t2