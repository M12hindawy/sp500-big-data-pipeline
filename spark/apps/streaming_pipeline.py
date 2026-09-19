"""
Spark Structured Streaming Pipeline for S&P 500 Historical Replay
Semantics & Dual-Time Model:
- trading_date: Preserved original historical business date from Kaggle.
- replay_time: Synthetic timestamp generated deterministically to simulate streaming ingestion.
- event_time: Streaming event-time derived directly from replay_time (event_time = replay_time).
- 5-Minute Replay Window: Grouping of replayed historical records by synthetic event_time.
  (Explicitly NOT real 5-minute market candles).
- Watermark: 10 minutes on synthetic event_time.
- Checkpoint: Persistent checkpoint at s3a://stock-data/checkpoints/streaming/
- Sinks: MinIO Gold Parquet + PostgreSQL table streaming_market_metrics via foreachBatch.
"""
import os
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, DateType
from common import (
    create_spark_session, KAFKA_EVENT_SCHEMA, get_postgres_properties
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-market-data")
CHECKPOINT_LOCATION = os.getenv("STREAMING_CHECKPOINT_PATH", "s3a://stock-data/checkpoints/streaming")
GOLD_STREAMING_PATH = os.getenv("GOLD_STREAMING_PATH", "s3a://stock-data/gold/streaming_market_metrics")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "5 minutes")
WATERMARK_DURATION = os.getenv("WATERMARK_DURATION", "10 minutes")

def process_microbatch(batch_df, batch_id):
    """
    ForeachBatch microbatch handler:
    1. Writes aggregated microbatch to MinIO Gold Parquet.
    2. Writes aggregated microbatch to PostgreSQL streaming_market_metrics.
    """
    row_count = batch_df.count()
    if row_count == 0:
        return

    print(f"[{batch_id}] Processing streaming microbatch with {row_count} windowed aggregations...")

    # 1. Write to MinIO Gold Parquet
    try:
        (
            batch_df.write
            .mode("append")
            .parquet(GOLD_STREAMING_PATH)
        )
        print(f"[{batch_id}] Successfully appended to Gold Parquet at {GOLD_STREAMING_PATH}")
    except Exception as e:
        print(f"[{batch_id}] Error writing to MinIO Gold: {e}")

    # 2. Write to PostgreSQL streaming_market_metrics
    try:
        pg_url, pg_properties = get_postgres_properties()
        
        # Prepare DataFrame for Postgres
        out_df = (
            batch_df
            .withColumn("processed_at", F.current_timestamp())
            .select(
                "symbol", "window_start", "window_end",
                "first_price", "max_price", "min_price", "last_price",
                "total_volume", "average_close", "price_change_pct",
                "average_intraday_range", "record_count", "source", "processed_at"
            )
        )

        out_df.write.jdbc(
            url=pg_url,
            table="stg_streaming_market_metrics",
            mode="overwrite",
            properties=pg_properties
        )

        # Idempotent upsert via staging table
        conn = batch_df._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            pg_url, pg_properties["user"], pg_properties["password"]
        )
        stmt = conn.createStatement()
        upsert_sql = """
            INSERT INTO streaming_market_metrics (
                symbol, window_start, window_end,
                first_price, max_price, min_price, last_price,
                total_volume, average_close, price_change_pct,
                average_intraday_range, record_count, source, processed_at
            )
            SELECT 
                symbol, window_start, window_end,
                first_price, max_price, min_price, last_price,
                total_volume, average_close, price_change_pct,
                average_intraday_range, record_count, source, processed_at
            FROM stg_streaming_market_metrics
            ON CONFLICT (symbol, window_start, window_end) DO UPDATE SET
                first_price = EXCLUDED.first_price,
                max_price = EXCLUDED.max_price,
                min_price = EXCLUDED.min_price,
                last_price = EXCLUDED.last_price,
                total_volume = EXCLUDED.total_volume,
                average_close = EXCLUDED.average_close,
                price_change_pct = EXCLUDED.price_change_pct,
                average_intraday_range = EXCLUDED.average_intraday_range,
                record_count = EXCLUDED.record_count,
                processed_at = EXCLUDED.processed_at;
        """
        stmt.execute(upsert_sql)
        stmt.close()
        conn.close()
        print(f"[{batch_id}] Successfully upserted {row_count} records into PostgreSQL streaming_market_metrics.")

    except Exception as e:
        print(f"[{batch_id}] Error writing to PostgreSQL: {e}")

def run_streaming_pipeline():
    print("============================================================")
    print("STARTING S&P 500 SPARK STRUCTURED STREAMING PIPELINE")
    print(f"Kafka Bootstrap:       {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic:           {KAFKA_TOPIC}")
    print(f"Checkpoint Path:       {CHECKPOINT_LOCATION}")
    print(f"Replay Window Duration: {WINDOW_DURATION}")
    print(f"Replay Watermark:      {WATERMARK_DURATION}")
    print("TIME MODEL:")
    print("  trading_date = Original Historical Business Date (Preserved)")
    print("  replay_time  = Synthetic Deterministic Replay Timestamp")
    print("  event_time   = replay_time (Used for Spark Watermark & Replay Windows)")
    print("============================================================")

    spark = create_spark_session("SP500_Structured_Streaming")
    spark.sparkContext.setLogLevel("WARN")

    # 1. READ FROM KAFKA
    kafka_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. PARSE JSON & DEFINE DUAL TIME MODEL
    parsed_df = (
        kafka_raw_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), KAFKA_EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Convert timestamps and enforce event_time = replay_time
    typed_df = (
        parsed_df
        .withColumn("trading_date", F.to_date(F.col("trading_date"), "yyyy-MM-dd"))
        .withColumn("replay_time", F.to_timestamp(F.col("replay_time")))
        .withColumn("event_time", F.to_timestamp(F.col("replay_time")))  # event_time = replay_time
    )

    # 3. FILTER VALID RECORDS
    valid_df = typed_df.filter(
        F.col("symbol").isNotNull() &
        F.col("trading_date").isNotNull() &
        F.col("event_time").isNotNull() &
        F.col("open").isNotNull() & (F.col("open") > 0) &
        F.col("high").isNotNull() & (F.col("high") > 0) &
        F.col("low").isNotNull() & (F.col("low") > 0) &
        F.col("close").isNotNull() & (F.col("close") > 0) &
        F.col("volume").isNotNull() & (F.col("volume") >= 0) &
        (F.col("high") >= F.col("low"))
    )

    # 4. WATERMARK ON SYNTHETIC EVENT TIME
    # Crucial: Watermark operates on event_time (derived from replay_time), NOT on trading_date
    watermarked_df = valid_df.withWatermark("event_time", WATERMARK_DURATION)

    # 5. 5-MINUTE REPLAY WINDOW AGGREGATION
    # Groups replayed historical events into synthetic 5-minute replay windows
    windowed_aggregations = (
        watermarked_df
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_time"), WINDOW_DURATION)
        )
        .agg(
            F.first("open").alias("first_price"),
            F.max("high").alias("max_price"),
            F.min("low").alias("min_price"),
            F.last("close").alias("last_price"),
            F.sum("volume").alias("total_volume"),
            F.round(F.avg("close"), 4).alias("average_close"),
            F.round(F.avg(F.col("high") - F.col("low")), 4).alias("average_intraday_range"),
            F.count("*").alias("record_count"),
            F.first("source").alias("source")
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn(
            "price_change_pct",
            F.round(
                F.when(F.col("first_price") > 0, ((F.col("last_price") - F.col("first_price")) / F.col("first_price")) * 100.0)
                .otherwise(0.0), 4
            )
        )
        .drop("window")
    )

    # 6. START STREAMING QUERY WITH FOREACHBATCH & PERSISTENT CHECKPOINT
    query = (
        windowed_aggregations.writeStream
        .foreachBatch(process_microbatch)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    timeout_sec = int(os.getenv("STREAMING_TIMEOUT_SECONDS", "0"))
    if timeout_sec > 0:
        print(f"Streaming query started with timeout of {timeout_sec} seconds. Processing microbatches...")
        query.awaitTermination(timeout_sec)
        print("Timeout reached. Streaming query completed gracefully.")
    else:
        print("Streaming query started in continuous mode. Awaiting termination...")
        query.awaitTermination()

if __name__ == "__main__":
    run_streaming_pipeline()
