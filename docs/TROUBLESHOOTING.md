# S&P 500 Data Pipeline - Troubleshooting Guide

Common issues, diagnostic commands, and recovery procedures.

---

## 1. Kafka Connection Issues

### Symptom
`NoBrokersAvailable` in producer logs or `org.apache.kafka.common.errors.TimeoutException` in Spark.

### Causes & Resolution
1. **Broker starting up**: Kafka KRaft mode takes ~5-10 seconds to elect metadata quorum and register listeners. Both `producer.py` and `streaming_pipeline.py` implement automatic retry backoff.
2. **DNS mismatch**: Ensure containers reference `kafka:9092` (internal Docker network) and host tools use `localhost:29092`.
3. **Verify broker status**:
   ```bash
   docker compose logs kafka
   docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
   ```

---

## 2. MinIO S3A Errors in Spark

### Symptom
`java.io.InterruptedIOException: doesBucketExist on stock-data` or `403 Forbidden`.

### Causes & Resolution
1. **Missing S3A configuration**: Ensure `spark.hadoop.fs.s3a.endpoint` is set to `http://minio:9000` with `path.style.access=true` and `ssl.enabled=false`.
2. **Bucket not initialized**: Run the initialization container:
   ```bash
   docker compose up minio-init
   ```
3. **Verify credentials**: Verify `AWS_ACCESS_KEY_ID=minioadmin` and `AWS_SECRET_ACCESS_KEY=minioadmin` match between `.env` and MinIO container.

---

## 3. Spark Memory / Out of Memory (OOM)

### Symptom
`java.lang.OutOfMemoryError: Java heap space` during batch execution.

### Causes & Resolution
1. **Unconstrained collect**: Do not call `.collect()` or `.toPandas()` on large multi-million row DataFrames.
2. **Tune driver memory**: In `.env`, adjust:
   ```env
   SPARK_DRIVER_MEMORY=3g
   SPARK_EXECUTOR_MEMORY=3g
   ```
3. **Control shuffle partitions**: Ensure `spark.sql.shuffle.partitions` is set to a reasonable number (4 to 8 for single-machine local mode).

---

## 4. Checkpoint Inconsistency in Structured Streaming

### Symptom
`IllegalStateException: Specific offset was not found` or schema mismatch upon restart.

### Causes & Resolution
1. **Schema changed**: If the Kafka event schema was modified, the old checkpoint cannot be re-used.
2. **Clear streaming checkpoint**:
   ```bash
   docker compose exec minio mc rm -r --force local/stock-data/checkpoints/streaming/
   ```
3. Restart streaming container:
   ```bash
   docker compose restart spark-streaming
   ```

---

## 5. PostgreSQL Upsert Deadlocks or Connection Exhaustion

### Symptom
`org.postgresql.util.PSQLException: FATAL: remaining connection slots are reserved for non-replication superuser connections`.

### Causes & Resolution
1. Spark JDBC writes are batched using `batchsize=1000` and `reWriteBatchedInserts=true`.
2. Ensure `max_connections` in Postgres is adequate (default 100 is sufficient for single-machine pipeline).
3. The staging table pattern (`stg_daily_market_metrics` -> `INSERT ... ON CONFLICT`) serializes writes into atomic transactions, preventing row-level deadlocks.
