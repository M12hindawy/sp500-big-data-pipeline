"""
Shared utilities, schemas, and transformations for S&P 500 Spark pipelines.
Ensures consistency between Batch and Structured Streaming transformations.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType, IntegerType
)
import pyspark.sql.functions as F

# Canonical Schema for Raw Stock CSV
STOCKS_RAW_SCHEMA = StructType([
    StructField("date", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("symbol", StringType(), False)
])

# Schema for S&P 500 Companies Metadata CSV
COMPANIES_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("company", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("sub_industry", StringType(), True),
    StructField("headquarters", StringType(), True),
    StructField("date_added", StringType(), True),
    StructField("founded", IntegerType(), True)
])

# Canonical Kafka Event JSON Schema
KAFKA_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("trading_date", StringType(), False),
    StructField("replay_time", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("source", StringType(), True)
])

def create_spark_session(app_name: str) -> SparkSession:
    """Creates a configured SparkSession with S3A (MinIO), Kafka, and Postgres support."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # S3A MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    return builder.getOrCreate()

def apply_validation_rules(df):
    """
    Evaluates row validity and adds is_valid and rejection_reason columns.
    Enforces:
    - Non-null symbol and trading_date
    - OHLC logic: high >= low, high >= open, high >= close, low <= open, low <= close
    - Positive prices and non-negative volume
    """
    valid_condition = (
        df["symbol"].isNotNull() & (F.trim(df["symbol"]) != "") &
        df["trading_date"].isNotNull() &
        df["open"].isNotNull() & (df["open"] > 0) &
        df["high"].isNotNull() & (df["high"] > 0) &
        df["low"].isNotNull() & (df["low"] > 0) &
        df["close"].isNotNull() & (df["close"] > 0) &
        df["volume"].isNotNull() & (df["volume"] >= 0) &
        (df["high"] >= df["low"]) &
        (df["high"] >= df["open"]) &
        (df["high"] >= df["close"]) &
        (df["low"] <= df["open"]) &
        (df["low"] <= df["close"])
    )

    rejection_reason = (
        F.when(df["symbol"].isNull() | (F.trim(df["symbol"]) == ""), "MISSING_SYMBOL")
        .when(df["trading_date"].isNull(), "MISSING_DATE")
        .when(df["high"] < df["low"], "HIGH_LESS_THAN_LOW")
        .when(df["high"] < df["open"], "HIGH_LESS_THAN_OPEN")
        .when(df["high"] < df["close"], "HIGH_LESS_THAN_CLOSE")
        .when(df["low"] > df["open"], "LOW_GREATER_THAN_OPEN")
        .when(df["low"] > df["close"], "LOW_GREATER_THAN_CLOSE")
        .when(df["volume"] < 0, "NEGATIVE_VOLUME")
        .when((df["open"] <= 0) | (df["close"] <= 0), "INVALID_PRICE")
        .otherwise("VALID")
    )

    return df.withColumn("is_valid", valid_condition).withColumn("rejection_reason", rejection_reason)

def calculate_derived_features(df):
    """
    Calculates pipeline-level financial and temporal features.
    Consistent across Batch and Streaming pipelines.
    """
    # Safe division helpers
    safe_pct_change = (
        F.when(df["open"] > 0, ((df["close"] - df["open"]) / df["open"]) * 100.0)
        .otherwise(0.0)
    )
    safe_intraday_pct = (
        F.when(df["open"] > 0, ((df["high"] - df["low"]) / df["open"]) * 100.0)
        .otherwise(0.0)
    )

    return (
        df
        .withColumn("symbol", F.upper(F.trim(df["symbol"])))
        .withColumn("price_change", F.round(df["close"] - df["open"], 4))
        .withColumn("price_change_pct", F.round(safe_pct_change, 4))
        .withColumn("intraday_range", F.round(df["high"] - df["low"], 4))
        .withColumn("intraday_range_pct", F.round(safe_intraday_pct, 4))
        .withColumn("typical_price", F.round((df["high"] + df["low"] + df["close"]) / 3.0, 4))
        .withColumn("dollar_volume", F.round(df["close"] * df["volume"].cast("double"), 2))
        .withColumn("year", F.year(df["trading_date"]))
        .withColumn("month", F.month(df["trading_date"]))
        .withColumn("day", F.dayofmonth(df["trading_date"]))
        .withColumn("day_of_week", F.dayofweek(df["trading_date"]))
        .withColumn("processed_at", F.current_timestamp())
    )

def get_postgres_properties():
    """Returns connection URL and properties for PostgreSQL serving layer."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT_INTERNAL", "5432")
    db = os.getenv("POSTGRES_DB", "stock_analytics")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
        "reWriteBatchedInserts": "true"
    }
    return url, properties
