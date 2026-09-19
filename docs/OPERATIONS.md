# S&P 500 Data Pipeline - Operations & Runbook

Step-by-step operational guide for deploying, managing, monitoring, and verifying the single-machine Big Data pipeline.

---

## 1. Quick Start Commands

### Start All Infrastructure
```bash
docker compose up -d
```

### Check Cluster Status
```bash
docker compose ps
```

### View Live Service Logs
```bash
# All services
docker compose logs -f

# Specific component
docker compose logs -f producer
docker compose logs -f spark-streaming
docker compose logs -f kafka
docker compose logs -f airflow
```

### Graceful Shutdown (Preserves Data)
```bash
docker compose down
```

> [!CAUTION]
> **Destructive Reset Command**:
> The following command deletes all persistent docker volumes (MinIO data lake, PostgreSQL database, Airflow metadata):
> ```bash
> docker compose down -v
> ```
> Use only when a complete wipe and fresh re-initialization is explicitly desired.

---

## 2. Component Verification Commands

### A. MinIO Object Storage
- Web Console: http://localhost:9001 (User: `minioadmin` / Pass: `minioadmin`)
- S3 API: http://localhost:9000
- CLI Inspection:
```bash
docker compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio mc ls local/stock-data/
docker compose exec minio mc ls -r local/stock-data/bronze/
```

### B. Apache Kafka (KRaft Mode)
- Kafka UI: http://localhost:8082
- Describe Topic:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic stock-market-data
```
- Sample Kafka Messages:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stock-market-data \
  --from-beginning \
  --max-messages 5
```

### C. PostgreSQL Serving Layer
- Port: `localhost:5432` (User: `postgres` / Pass: `postgres` / DB: `stock_analytics`)
- Execute Query:
```bash
docker compose exec postgres psql -U postgres -d stock_analytics -c "SELECT COUNT(*) FROM daily_market_metrics;"
docker compose exec postgres psql -U postgres -d stock_analytics -c "SELECT * FROM vw_top_volume LIMIT 5;"
docker compose exec postgres psql -U postgres -d stock_analytics -c "SELECT * FROM pipeline_runs ORDER BY start_time DESC LIMIT 3;"
```

### D. Apache Spark Execution
- Run Spark Batch Job manually:
```bash
docker compose exec spark python /app/apps/batch_pipeline.py
```
- Run Spark Streaming Job manually:
```bash
docker compose exec spark python /app/apps/streaming_pipeline.py
```

### E. Apache Airflow
- Web UI: http://localhost:8080 (User: `admin` / Pass: generated or default)
- List Active DAGs:
```bash
docker compose exec airflow airflow dags list
```
- Trigger Batch Pipeline DAG:
```bash
docker compose exec airflow airflow dags trigger daily_sp500_pipeline
```

---

## 3. End-to-End Demo Workflow

1. **Initialize Data**:
   Ensure `data/sp500_stocks.csv` and `data/sp500_companies.csv` are in place (or run `python data/generate_sample.py`).
2. **Start Infrastructure**:
   `docker compose up -d`
3. **Trigger Batch Pipeline**:
   `docker compose exec spark python /app/apps/batch_pipeline.py`
4. **Inspect Parquet in MinIO**:
   Verify `s3a://stock-data/silver/stocks/` and `s3a://stock-data/gold/daily_market_metrics/`.
5. **Start Replay Streaming**:
   `docker compose up -d producer spark-streaming`
6. **Watch Real-Time Microbatches**:
   `docker compose logs -f spark-streaming`
7. **Query PostgreSQL**:
   Inspect `streaming_market_metrics` table and `vw_streaming_metrics` view.
