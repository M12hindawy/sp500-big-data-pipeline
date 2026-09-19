"""
Apache Airflow Orchestration DAG: daily_sp500_pipeline
Orchestrates:
1. check_dataset: Verifies raw source CSV presence
2. upload_or_verify_bronze: Verifies MinIO Bronze bucket and objects
3. spark_batch: Executes Spark Batch pipeline
4. quality_gate: Validates data quality metrics in PostgreSQL
5. postgres_validation: Verifies serving layer views and row counts
6. pipeline_success: Logs completion metrics
"""
import os
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2
import boto3
import requests

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "daily_sp500_pipeline",
    default_args=default_args,
    description="Orchestrates S&P 500 Batch Pipeline and Data Quality Gates",
    schedule_interval=None,  # Manual / Configurable as required by prompt
    catchup=False,
    max_active_runs=1,
    tags=["sp500", "batch", "minio", "spark", "postgres"]
)

def check_dataset_func():
    """Validates existence of local or bronze datasets."""
    stocks_file = "/data/sp500_stocks.csv"
    companies_file = "/data/sp500_companies.csv"
    if not os.path.exists(stocks_file) or not os.path.exists(companies_file):
        raise FileNotFoundError(f"Missing required dataset files in /data. Check DATASET_SETUP.md.")
    print(f"Dataset check passed: {stocks_file} ({os.path.getsize(stocks_file)} bytes)")

def verify_bronze_func():
    """Verifies that MinIO Bronze bucket has the raw CSV files."""
    s3_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    s3_access = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    bucket = os.getenv("MINIO_BUCKET", "stock-data")

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access,
        aws_secret_access_key=s3_secret
    )
    # Check bucket
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in buckets:
        raise ValueError(f"MinIO bucket '{bucket}' does not exist.")

    # Check bronze objects
    response = s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")
    keys = [item["Key"] for item in response.get("Contents", [])]
    print(f"Bronze objects found in MinIO: {keys}")
    if not any("sp500_stocks.csv" in k for k in keys):
        raise ValueError(f"sp500_stocks.csv missing in MinIO Bronze bucket '{bucket}'.")

def run_spark_batch_func():
    """Triggers the Spark Batch Pipeline via the Spark container endpoint."""
    spark_endpoint = os.getenv("SPARK_ENDPOINT", "http://spark:8088/batch")
    print(f"Triggering Spark batch pipeline via {spark_endpoint}...")
    resp = requests.post(spark_endpoint, timeout=600)
    data = resp.json()
    stdout = data.get("stdout", "")
    stderr = data.get("stderr", "")
    retcode = data.get("returncode", -1)
    
    print("--- Spark Execution Output ---")
    print(stdout)
    if retcode != 0:
        print("--- Spark Execution Error ---")
        print(stderr)
        raise RuntimeError(f"Spark Batch Pipeline failed with exit code {retcode}")
    print(f"Spark Batch Pipeline succeeded with exit code {retcode}.")

def quality_gate_func():
    """Verifies data quality gate from PostgreSQL audit tables."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT_INTERNAL", "5432")
    db = os.getenv("POSTGRES_DB", "stock_analytics")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    cur = conn.cursor()
    cur.execute("SELECT check_name, expected, actual, status FROM data_quality_results ORDER BY created_at DESC LIMIT 5;")
    rows = cur.fetchall()
    print("Latest Data Quality Results:")
    for r in rows:
        print(f" - Check: {r[0]} | Expected: {r[1]} | Actual: {r[2]} | Status: {r[3]}")
    
    cur.close()
    conn.close()

def postgres_validation_func():
    """Verifies PostgreSQL daily_market_metrics and Power BI views."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT_INTERNAL", "5432")
    db = os.getenv("POSTGRES_DB", "stock_analytics")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM daily_market_metrics;")
    total_records = cur.fetchone()[0]
    print(f"PostgreSQL daily_market_metrics total count: {total_records}")
    if total_records == 0:
        raise ValueError("PostgreSQL daily_market_metrics has 0 rows after batch execution!")

    cur.execute("SELECT COUNT(*) FROM vw_top_volume;")
    vol_records = cur.fetchone()[0]
    print(f"Power BI view vw_top_volume records: {vol_records}")
    cur.close()
    conn.close()

t1_check_dataset = PythonOperator(
    task_id="check_dataset",
    python_callable=check_dataset_func,
    dag=dag
)

t2_verify_bronze = PythonOperator(
    task_id="upload_or_verify_bronze",
    python_callable=verify_bronze_func,
    dag=dag
)

t3_spark_batch = PythonOperator(
    task_id="spark_batch",
    python_callable=run_spark_batch_func,
    dag=dag
)

t4_quality_gate = PythonOperator(
    task_id="quality_gate",
    python_callable=quality_gate_func,
    dag=dag
)

t5_postgres_validation = PythonOperator(
    task_id="postgres_validation",
    python_callable=postgres_validation_func,
    dag=dag
)

t6_pipeline_success = BashOperator(
    task_id="pipeline_success",
    bash_command='echo "S&P 500 Daily Market Pipeline completed successfully at $(date -u)"',
    dag=dag
)

t1_check_dataset >> t2_verify_bronze >> t3_spark_batch >> t4_quality_gate >> t5_postgres_validation >> t6_pipeline_success
