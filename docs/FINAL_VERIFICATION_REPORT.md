# Walkthrough: S&P 500 Big Data Pipeline - Full Live Execution & Acceptance Report

This document records the complete, verified live execution of the single-machine S&P 500 Big Data Pipeline across MinIO, Kafka KRaft, Spark (Batch & Structured Streaming), PostgreSQL, and Apache Airflow.

---

## 1. Live Infrastructure Status (`docker compose ps`)

All required services are running and healthy inside Docker Compose on `stock-pipeline-net`:

| Container Name | Service | Status | Ports / Endpoint | Verified Health |
|---|---|---|---|---|
| `sp500-minio` | MinIO S3 Object Lake | Up (healthy) | `9000` (API), `9001` (Console) | Healthy |
| `sp500-kafka` | Apache Kafka (KRaft) | Up (healthy) | `9092` (internal), `29092` (host) | Healthy (Topic `stock-market-data`, 3 partitions, RF=1) |
| `sp500-kafka-ui` | Kafka UI Monitor | Up | `8082` (host) | Up & accessible |
| `sp500-postgres` | PostgreSQL Serving DB | Up (healthy) | `5432` (host) | Healthy (`stock_analytics` DB) |
| `sp500-spark` | Apache Spark 3.5.6 | Up | Internal / local execution | PySpark runtime with S3A, Kafka, Postgres JARs |
| `sp500-producer` | Historical Replay Producer | Exited (code 0) | Internal | Read 600, published 600 records deterministically |
| `sp500-airflow` | Apache Airflow 2.10.0 | Up | `8080` (host) | Healthy (DAGs loaded & tested) |

---

## 2. MinIO Object Lake Verification

Verified via MinIO S3 inspection:
```text
--- MINIO LAKE INSPECTION ---
Buckets: ['stock-data']

Prefix 'bronze/': 2 objects
  - bronze/company/sp500_companies.csv
  - bronze/stocks/sp500_stocks.csv

Prefix 'silver/': 4 objects (Partitioned by year and month)
  - silver/stocks/_SUCCESS
  - silver/stocks/year=2020/month=1/part-00000-...snappy.parquet
  - silver/stocks/year=2020/month=2/part-00000-...snappy.parquet
  - silver/stocks/year=2020/month=3/part-00000-...snappy.parquet

Prefix 'gold/': 7 objects (Parquet aggregates)
  - gold/daily_market_metrics/_SUCCESS
  - gold/daily_market_metrics/part-00000-...snappy.parquet
  - gold/streaming_market_metrics/_SUCCESS
  - gold/streaming_market_metrics/part-00000-...snappy.parquet
  - gold/streaming_market_metrics/part-00001-...snappy.parquet

Prefix 'checkpoints/': 15 objects (Persistent Spark state store)
  - checkpoints/streaming/commits/0
  - checkpoints/streaming/commits/1
  - checkpoints/streaming/metadata
  - checkpoints/streaming/offsets/0
  - checkpoints/streaming/offsets/1
```

---

## 3. Spark Batch Execution Evidence

Execution logs from `docker exec sp500-spark python3 /app/apps/batch_pipeline.py`:

```text
[2026-09-14 18:59:32.201571] Starting S&P 500 Batch Pipeline (Run ID: d8c4cac1)
Reading raw stocks from s3a://stock-data/bronze/stocks/sp500_stocks.csv...
Total raw observations read: 600
Validation summary: Valid=600 | Rejected=0
Deduplication: Kept=600 | Removed Duplicates=0
Reading company metadata from s3a://stock-data/bronze/company/sp500_companies.csv...
Writing Silver Parquet to s3a://stock-data/silver/stocks...
Silver Parquet write completed.
Writing Gold Parquet to s3a://stock-data/gold/daily_market_metrics...
Gold Parquet write completed.
Writing Gold table to PostgreSQL (daily_market_metrics)...
PostgreSQL daily_market_metrics upsert completed successfully.
============================================================
SPARK BATCH PIPELINE FINISHED SUCCESSFULLY
Run ID:        d8c4cac1
Input Rows:    600
Output Rows:   600
Rejected Rows: 0
============================================================
```

---

## 4. Historical Producer & Kafka Replay Evidence

Execution logs from `sp500-producer`:
```text
S&P 500 HISTORICAL STREAMING REPLAY PRODUCER STARTING
Target Topic: stock-market-data
Data File: /data/sp500_stocks.csv
Rate: 100.0 records/sec
Replay Start Time: 2000-01-01T00:00:00
Replay Interval: 1.0s per record
Successfully connected to Kafka broker.
============================================================
PRODUCER REPLAY RUN COMPLETED
TOTAL READ:        600
TOTAL PUBLISHED:   600
TOTAL REJECTED:    0
KAFKA ERRORS:      0
TOTAL TIME:        6.44 seconds
AVG PUBLISH RATE:  93.2 records/second
============================================================
```

Sample consumed messages from Kafka topic:
```json
{"event_id": "e1ad4f5dadff8919", "symbol": "AMZN", "trading_date": "2020-01-02", "replay_time": "2000-01-01T00:00:03", "event_time": "2000-01-01T00:00:03", "open": 95.0, "high": 97.5, "low": 93.2, "close": 95.9, "adj_close": 95.9, "volume": 1080000, "source": "historical_replay"}
{"event_id": "e863bf6878fbf50c", "symbol": "NVDA", "trading_date": "2020-01-02", "replay_time": "2000-01-01T00:00:04", "event_time": "2000-01-01T00:00:04", "open": 59.0, "high": 61.5, "low": 57.2, "close": 59.9, "adj_close": 59.9, "volume": 1080000, "source": "historical_replay"}
{"event_id": "776749e304877e01", "symbol": "JPM", "trading_date": "2020-01-02", "replay_time": "2000-01-01T00:00:05", "event_time": "2000-01-01T00:00:05", "open": 138.0, "high": 140.5, "low": 136.2, "close": 138.9, "adj_close": 138.9, "volume": 1060000, "source": "historical_replay"}
```

---

## 5. Spark Structured Streaming & Replay Windows Evidence

Execution logs from `streaming_pipeline.py`:
```text
============================================================
STARTING S&P 500 SPARK STRUCTURED STREAMING PIPELINE
Kafka Bootstrap:       kafka:9092
Kafka Topic:           stock-market-data
Checkpoint Path:       s3a://stock-data/checkpoints/streaming
Replay Window Duration: 5 minutes
Replay Watermark:      10 minutes
TIME MODEL:
  trading_date = Original Historical Business Date (Preserved)
  replay_time  = Synthetic Deterministic Replay Timestamp
  event_time   = replay_time (Used for Spark Watermark & Replay Windows)
============================================================
Streaming query started with timeout of 30 seconds. Processing microbatches...
[0] Processing streaming microbatch with 20 windowed aggregations...
[0] Successfully appended to Gold Parquet at s3a://stock-data/gold/streaming_market_metrics
[0] Successfully upserted 20 records into PostgreSQL streaming_market_metrics.
Timeout reached. Streaming query completed gracefully.
```

---

## 6. PostgreSQL Serving Layer & Power BI Views

Verified live in PostgreSQL database `stock_analytics`:

### Table `daily_market_metrics`:
- **Total Records**: 600
- **Null Keys**: 0
- **Invalid OHLC**: 0
- **Duplicates**: 0

### Table `streaming_market_metrics`:
- **Total Windows**: 20 distinct 5-minute replay simulation windows
- **Window Durations**: Exactly 300 seconds (`00:00:00 -> 00:05:00`, `00:05:00 -> 00:10:00`)
- **Source**: `historical_replay`

### View `vw_latest_stock_metrics`:
```
 symbol |         company         |         sector         | trading_date | close_price | price_change_pct 
--------+-------------------------+------------------------+--------------+-------------+------------------
 AAPL   | Apple Inc.              | Information Technology | 2020-03-25   |     90.6500 |           1.0028
 AMZN   | Amazon.com Inc.         | Consumer Discretionary | 2020-03-25   |    110.6500 |           0.8200
 GOOGL  | Alphabet Inc. (Class A) | Communication Services | 2020-03-25   |     83.6500 |           1.0876
 JNJ    | Johnson & Johnson       | Health Care            | 2020-03-25   |    160.6500 |           0.5634
 JPM    | JPMorgan Chase & Co.    | Financials             | 2020-03-25   |    153.6500 |           0.5892
```

### Table `pipeline_runs`:
```
  run_id  |    pipeline_name     | status  | input_rows | output_rows | rejected_rows 
----------+----------------------+---------+------------+-------------+---------------
 d8c4cac1 | SP500_Batch_Pipeline | SUCCESS |        600 |         600 |             0
```

### Table `data_quality_results`:
```
 check_name   | expected | actual | status 
--------------+----------+--------+--------
 ROW_COUNT    | > 0      | 600    | PASSED
 REJECTED_ROWS| 0        | 0      | PASSED
 DUPLICATES   | 0        | 0      | PASSED
```

---

## 7. Airflow Orchestration Evidence

Execution of DAG `pipeline_health_check` in `sp500-airflow`:
```text
Task: check_kafka    -> Kafka Healthy. Topics found: {'stock-market-data'} [SUCCESS]
Task: check_minio    -> MinIO Healthy. Buckets found: ['stock-data'] [SUCCESS]
Task: check_postgres -> PostgreSQL Healthy. Version: PostgreSQL 16.15 [SUCCESS]
DagRun Finished: state=success, run_duration=1.65s
```

---

## 8. Checkpoint Recovery Test Evidence

1. Triggered `streaming_pipeline.py` with existing checkpoint at `s3a://stock-data/checkpoints/streaming/`.
2. Loaded committed offsets (`offsets/0`, `offsets/1`).
3. Re-evaluated Kafka queue: recognized all messages already processed.
4. Completed gracefully without error.
5. Queried PostgreSQL `streaming_market_metrics`: count remained exactly 20 (zero duplicate insertions).

---

## 9. End-to-End Airflow Orchestration Execution (`daily_sp500_pipeline`)

Command: `docker exec sp500-airflow airflow dags test daily_sp500_pipeline`

All 6 pipeline tasks executed in strict sequence and passed:
1. `check_dataset`: Validated presence and size of `/data/sp500_stocks.csv` and `/data/sp500_companies.csv` [SUCCESS]
2. `upload_or_verify_bronze`: Verified MinIO bucket `stock-data` and objects `bronze/stocks/sp500_stocks.csv` [SUCCESS]
3. `spark_batch`: Triggered Spark container job runner via internal REST endpoint; processed 600 rows (Input: 600, Output: 600, Rejected: 0) [SUCCESS]
4. `quality_gate`: Validated audit logs in PostgreSQL (`ROW_COUNT > 0`, `REJECTED_ROWS == 0`, `DUPLICATES == 0`) [SUCCESS]
5. `postgres_validation`: Validated PostgreSQL row count (600) and Power BI view `vw_top_volume` (20 rows) [SUCCESS]
6. `pipeline_success`: Logged pipeline completion timestamp [SUCCESS]

**Result**: `DagRun Finished: dag_id=daily_sp500_pipeline, state=success, run_duration=22.73s`

---

## 10. Automated Test Suite Verification (20 / 20 PASSED)

Command: `docker exec sp500-spark pytest /app/tests -v`

```text
tests/test_batch_stream_consistency.py::test_single_observation_window_equivalence PASSED [  5%]
tests/test_data_quality.py::test_valid_record PASSED                     [ 10%]
tests/test_data_quality.py::test_high_less_than_low_rejected PASSED      [ 15%]
tests/test_data_quality.py::test_negative_volume_rejected PASSED         [ 20%]
tests/test_data_quality.py::test_missing_symbol_rejected PASSED          [ 25%]
tests/test_data_quality.py::test_invalid_negative_price_rejected PASSED  [ 30%]
tests/test_schema.py::test_raw_csv_schema_fields PASSED                  [ 35%]
tests/test_schema.py::test_company_metadata_schema_fields PASSED         [ 40%]
tests/test_schema.py::test_kafka_event_schema_contract PASSED            [ 45%]
tests/test_streaming_time_semantics.py::test_1_trading_date_preservation PASSED [ 50%]
tests/test_streaming_time_semantics.py::test_2_and_test_3_deterministic_replay_time PASSED [ 55%]
tests/test_streaming_time_semantics.py::test_4_event_time_derived_from_replay_time PASSED [ 60%]
tests/test_streaming_time_semantics.py::test_5_and_test_6_same_trading_date_different_replay_windows PASSED [ 65%]
tests/test_streaming_time_semantics.py::test_7_watermark_uses_event_time PASSED [ 70%]
tests/test_streaming_time_semantics.py::test_8_explicit_replay_window_semantics PASSED [ 75%]
tests/test_streaming_time_semantics.py::test_9_no_wall_clock_time_used_in_producer_event_generation PASSED [ 80%]
tests/test_streaming_time_semantics.py::test_10_checkpointing_persisted PASSED [ 85%]
tests/test_transformations.py::test_price_change_calculations PASSED     [ 90%]
tests/test_transformations.py::test_loss_calculations PASSED             [ 95%]
tests/test_transformations.py::test_zero_open_safe_division PASSED       [100%]

======================== 20 passed in 0.13s ========================
```

---

## 11. Resilience & Failure Recovery Testing

- **PostgreSQL Container Crash Drill**:
  - Command: `docker restart sp500-postgres`
  - Validation: Queried `SELECT COUNT(*) FROM daily_market_metrics;` immediately after restart.
  - Result: Returned exactly 600 records initially, and 25,500 records on the scaled dataset. Zero data loss, volume mounting persisted data cleanly.
- **Kafka & MinIO Health Checks**:
  - Airflow DAG `pipeline_health_check` confirms end-to-end connectivity across all services.

---

## 12. Scaled Dataset Pipeline Execution (25,500 Observations)

A scaled verification run was executed across **51 companies** representing all 11 GICS economic sectors over **500 historical trading days** (25,500 total observations, 1.4 MB CSV):

### MinIO Bronze Ingestion:
- `bronze/stocks/sp500_stocks.csv`: 1.38 MiB transferred at 2.41 MiB/s.
- `bronze/company/sp500_companies.csv`: 4.98 KiB transferred at 17.76 KiB/s.

### Spark Batch Pipeline:
- Command: `docker exec sp500-spark python3 /app/apps/batch_pipeline.py`
- Run ID: `db906eaa` | Input Rows: 25,500 | Output Rows: 25,500 | Rejected Rows: 0
- Silver Tier: Partitioned Parquet written by `year` and `month`.
- Gold Tier: Daily market metrics Parquet written to MinIO.
- Serving Tier: 25,500 rows upserted into PostgreSQL `daily_market_metrics` in 27.48 seconds.

### Kafka Streaming Producer:
- Command: `docker compose run --rm -e PRODUCER_MAX_RECORDS=25500 -e PRODUCER_RATE=2000 producer`
- Emitted: 25,500 records at **1,135.5 records/second** in 22.46 seconds.
- Replay Time progression: `2000-01-01T00:00:00` -> `2000-01-01T07:05:00` (strictly deterministic).
- Partitions: Distributed across 3 partitions via ticker `symbol` key.

### Spark Structured Streaming Processing:
- Command: `docker exec -e STREAMING_TIMEOUT_SECONDS=45 sp500-spark python3 /app/apps/streaming_pipeline.py`
- Generated: **4,335 windowed aggregations** across 5-minute replay simulation windows.
- Gold Tier: Appended to `s3a://stock-data/gold/streaming_market_metrics`.
- Serving Tier: Upserted 4,335 records into PostgreSQL `streaming_market_metrics`.

---

## 13. Power BI Views Verification & Query Performance

The 6 PostgreSQL analytical views were verified using `EXPLAIN ANALYZE`:

| Analytical View | Target BI Dashboard | Query Latency | Execution Plan |
|---|---|---|---|
| `vw_latest_stock_metrics` | Executive Overview Card / Matrix | 32.4 ms | Seq Scan + Sort (25,500 rows) |
| `vw_top_volume` | Liquidity Leaders Bar Chart | 11.2 ms | Top-N heapsort limit 20 |
| `vw_top_gainers` | Top 10 Gainers Tile | 8.4 ms | Top-N heapsort limit 10 |
| `vw_top_losers` | Top 10 Losers Tile | 8.1 ms | Top-N heapsort limit 10 |
| `vw_streaming_metrics` (Symbol) | Real-time Candle / Replay Window | **0.145 ms** | `Index Scan using idx_smm_symbol` |
| `vw_pipeline_health` | Operational SLA & Health Monitor | 0.8 ms | Table scan on `pipeline_runs` |

---

## 14. Scaled Idempotency & Checkpoint Recovery Verification

1. **Batch Idempotency**: Re-running the batch pipeline on the 25,500-row dataset resulted in `daily_market_metrics` count remaining exactly **25,500** (0 duplicates created).
2. **Streaming Checkpoint Recovery**: Re-running the streaming pipeline against the Kafka topic with existing MinIO checkpoints read 0 uncommitted offsets, completed gracefully, and left `streaming_market_metrics` at exactly **4,335** rows.

---

## 15. Complete 10–15 Minute Video Demonstration Guide

The complete scene-by-scene presentation blueprint, narrator voiceover script in both English and Arabic, live terminal commands, and visual proofs are documented in:
- [DEMO_VIDEO_SCRIPT.md](DEMO_VIDEO_SCRIPT.md)
