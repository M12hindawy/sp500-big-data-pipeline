# S&P 500 Data Pipeline - Failure & Recovery Testing

Testing single-machine component failures, observing system behavior, and verifying graceful recovery.

> [!NOTE]
> **Single-Machine Fault Tolerance Scope**:
> In a single-machine environment, restarts demonstrate process-level resilience, automatic reconnection, and state recovery from persistent volumes and checkpoints. They do NOT provide physical hardware fault tolerance.

---

## 1. Test Matrix Summary

| Component | Failure Simulated | Visible Symptom | Recovery Action | Verified Result |
|---|---|---|---|---|
| **Kafka Broker** | `docker stop kafka` | Producer retries with backoff; Spark stream pauses | `docker start kafka` | Broker re-elects KRaft quorum; Producer resumes sending; Spark resumes microbatches without record loss. |
| **Historical Producer** | `docker stop producer` | Ingestion stops; no new messages in Kafka | `docker start producer` | Producer resumes; reads remaining records; Kafka partition offsets continue sequentially. |
| **Spark Streaming** | `docker stop spark-streaming` | Microbatches halt; Kafka consumer lag grows | `docker start spark-streaming` | Loads existing checkpoint from MinIO; reads uncommitted Kafka offsets; continues updates to PostgreSQL. |
| **MinIO Storage** | `docker stop minio` | S3A writes fail; Checkpoint inaccessible | `docker start minio` | Buckets and Parquet data persist on disk volume; Spark reconnects and writes resume. |
| **PostgreSQL** | `docker stop postgres` | Microbatch JDBC write fails with connection exception | `docker start postgres` | Tables and views persist; next microbatch retries and completes successfully. |
| **Airflow** | `docker stop airflow` | UI unavailable; scheduled tasks pause | `docker start airflow` | Webserver & scheduler reload DAGs; execution history preserved in Postgres. |

---

## 2. Step-by-Step Failure Drills

### Drill 1: Spark Structured Streaming Checkpoint Recovery
1. Launch streaming job:
   ```bash
   docker compose up -d spark-streaming producer
   ```
2. Wait until several microbatches complete (check PostgreSQL `SELECT count(*) FROM streaming_market_metrics`).
3. Kill Spark container:
   ```bash
   docker compose kill spark-streaming
   ```
4. Verify checkpoint files exist in MinIO at `stock-data/checkpoints/streaming/commits/` and `offsets/`.
5. Restart Spark container:
   ```bash
   docker compose start spark-streaming
   ```
6. **Observed Result**: Spark recovers previous state, resumes from the exact Kafka offset, and continues populating `streaming_market_metrics` without generating duplicate keys due to the unique constraint on `(symbol, window_start, window_end)`.

### Drill 2: Kafka Broker Outage & Reconnection
1. Stop Kafka container:
   ```bash
   docker compose stop kafka
   ```
2. Observe Producer logs:
   `WARNING [SP500HistoricalProducer] Kafka broker not available yet. Retrying in 2s...`
3. Restart Kafka container:
   ```bash
   docker compose start kafka
   ```
4. **Observed Result**: Within 5 seconds, the producer reconnects and flushes buffered batches. No crash occurs.

### Drill 3: MinIO Lake Persistence
1. Stop MinIO:
   ```bash
   docker compose stop minio
   ```
2. Restart MinIO:
   ```bash
   docker compose start minio
   ```
3. Verify files:
   ```bash
   docker compose exec minio mc ls local/stock-data/silver/stocks/
   ```
4. **Observed Result**: All Parquet partitions in Silver and Gold remain fully intact and readable.
