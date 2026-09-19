"""
Pipeline Health Check DAG
Periodically validates availability of MinIO, Kafka, and PostgreSQL services.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import boto3
from kafka import KafkaConsumer

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    "pipeline_health_check",
    default_args=default_args,
    description="Heartbeat health checks for MinIO, Kafka, and PostgreSQL",
    schedule_interval=None,  # Configurable
    catchup=False,
    tags=["monitoring", "health"]
)

def check_minio_health():
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3 = boto3.client("s3", endpoint_url=s3_endpoint,
                      aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                      aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    buckets = s3.list_buckets()
    print(f"MinIO Healthy. Buckets found: {[b['Name'] for b in buckets.get('Buckets', [])]}")

def check_kafka_health():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = KafkaConsumer(bootstrap_servers=bootstrap.split(","), request_timeout_ms=5000)
    topics = consumer.topics()
    consumer.close()
    print(f"Kafka Healthy. Topics found: {topics}")

def check_postgres_health():
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT_INTERNAL", "5432")
    db = os.getenv("POSTGRES_DB", "stock_analytics")
    conn = psycopg2.connect(host=host, port=port, dbname=db,
                            user=os.getenv("POSTGRES_USER", "postgres"),
                            password=os.getenv("POSTGRES_PASSWORD", "postgres"))
    cur = conn.cursor()
    cur.execute("SELECT version();")
    v = cur.fetchone()[0]
    print(f"PostgreSQL Healthy. Version: {v}")
    cur.close()
    conn.close()

check_minio = PythonOperator(task_id="check_minio", python_callable=check_minio_health, dag=dag)
check_kafka = PythonOperator(task_id="check_kafka", python_callable=check_kafka_health, dag=dag)
check_postgres = PythonOperator(task_id="check_postgres", python_callable=check_postgres_health, dag=dag)

[check_minio, check_kafka, check_postgres]
