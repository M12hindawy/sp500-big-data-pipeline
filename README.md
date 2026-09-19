# S&P 500 Stocks Big Data Pipeline (Single-Machine Complete Implementation)

An enterprise-grade, single-machine Big Data processing pipeline for the **25-Year S&P 500 Stocks dataset**, built using Docker Compose, Apache Spark (Batch + Structured Streaming), Apache Kafka (KRaft), MinIO (S3 Object Lake), PostgreSQL (Serving Layer), and Apache Airflow (Workflow Orchestration).

---

## 1. Project Overview

This pipeline ingests, validates, cleans, transforms, aggregates, and serves 25 years of daily S&P 500 stock market data (~3M+ records). It demonstrates both:
1. **Spark Batch Processing**: Incremental/full processing through Bronze, Silver, and Gold Parquet layers to PostgreSQL.
2. **Historical Streaming Simulation**: Replaying daily historical observations through Kafka into Spark Structured Streaming with deterministic event-time progression, watermarking, and 5-minute replay window aggregations.

> [!IMPORTANT]
> **Replay Time Semantics Notice**:  
> The replay time is synthetic and exists only to simulate streaming behavior. It is not real market timestamp data.

---

## 2. Dataset

- **Name**: S&P 500 Stocks: 25 Years of Data (Updated Daily)
- **Kaggle URL**: https://www.kaggle.com/datasets/darkmatternet/s-and-p-500-stocks-25-years-of-data-updated-daily
- **Files**:
  - `sp500_stocks.csv`: Daily OHLCV records (`date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`, `symbol`).
  - `sp500_companies.csv`: Company reference metadata (`symbol`, `company`, `sector`, `sub_industry`, `headquarters`, `date_added`, `founded`).
- **Setup Guide**: See [DATASET_SETUP.md](DATASET_SETUP.md).

---

## 3. Architecture & Data Flow

```
                    KAGGLE DATASET (sp500_stocks.csv, sp500_companies.csv)
                                       |
                                       v
                             MINIO BRONZE (S3 Object Lake)
                                       |
                   +-------------------+-------------------+
                   |                                       |
                   v                                       v
              SPARK BATCH                           PYTHON PRODUCER
                   |                                       |
                   |                                       v
                   |                                  KAFKA KRAFT
                   |                                       |
                   |                                       v
                   |                            SPARK STRUCTURED STREAMING
                   |                                       |
                   +-------------------+-------------------+
                                       |
                                       v
                             VALIDATION & CLEANING
                                       |
                                       v
                             FEATURE TRANSFORMATIONS
                                       |
                                       v
                            AGGREGATIONS & SPARK SQL
                                       |
                        +--------------+--------------+
                        |                             |
                        v                             v
                   MINIO SILVER                  MINIO GOLD
               (Partitioned Parquet)          (Parquet Analytics)
                                                      |
                                                      v
                                              POSTGRESQL SERVING
                                                      |
                                                      v
                                           POWER BI / ANALYTICS VIEWS
```

---

## 4. Docker Compose Services & Ports

| Service | Container Name | Host Port | Internal Port | Purpose |
|---|---|---|---|---|
| **MinIO** | `sp500-minio` | `9000`, `9001` | `9000`, `9001` | S3-compatible Bronze, Silver, Gold, and Checkpoints store |
| **Kafka** | `sp500-kafka` | `29092` | `9092` | Event streaming engine in KRaft mode (no Zookeeper) |
| **Kafka UI** | `sp500-kafka-ui` | `8082` | `8080` | Real-time topic, partition, and consumer lag monitor |
| **PostgreSQL**| `sp500-postgres` | `5432` | `5432` | Analytical serving layer with Power BI SQL views |
| **Spark** | `sp500-spark` | - | - | PySpark 3.5.6 Batch & Structured Streaming runtime |
| **Producer** | `sp500-producer` | - | - | Historical streaming replay generator |
| **Airflow** | `sp500-airflow` | `8080` | `8080` | Batch pipeline orchestration and data quality gates |

---

## 5. Dual-Time Model & Streaming Semantics

The source data is daily market records. To accurately simulate streaming without confounding historical dates with real-time ticks:

1. **`trading_date`**: Original business date from Kaggle (e.g., `2020-01-02`). Preserved unmolested across all layers.
2. **`replay_time`**: Synthetic timestamp generated deterministically by the Python Kafka producer:
   $$\text{replay\_time} = \text{REPLAY\_START\_TIME} + (\text{record\_index} \times \text{REPLAY\_INTERVAL\_SECONDS})$$
   *Never generated from system wall-clock time.*
3. **`event_time`**: Streaming event-time used by Spark: $\text{event\_time} = \text{replay\_time}$.
4. **5-Minute Replay Window**:
   - `window("event_time", "5 minutes")`
   - Explicitly defined as a **5-Minute Replay Simulation Window**, NOT a real 5-minute market candle.
   - Watermark configured on synthetic event-time: `withWatermark("event_time", "10 minutes")`.

---

## 6. Commands Reference

### Start Infrastructure
```bash
docker compose up -d
```

### Inspect Status
```bash
docker compose ps
```

### Run Automated Tests
```bash
python tests/run_all_tests.py
```

### Run Spark Batch Pipeline
```bash
docker compose exec spark python /app/apps/batch_pipeline.py
```

### Run Spark Structured Streaming
```bash
docker compose exec spark python /app/apps/streaming_pipeline.py
```

### Stop Infrastructure (Preserves Data)
```bash
docker compose down
```

### Reset Everything (Destructive - deletes volumes)
```bash
docker compose down -v
```

---

## 7. Power BI & Analytics SQL Views

PostgreSQL provides instant analytics views ready for Power BI:
- `vw_latest_stock_metrics`: Latest close, price change, and sector classification per symbol.
- `vw_top_volume`: Top 20 most liquid stocks by dollar traded volume.
- `vw_top_gainers`: Top percentage gainers.
- `vw_top_losers`: Top percentage losers.
- `vw_streaming_metrics`: Live 5-minute replay window aggregations.
- `vw_pipeline_health`: Airflow and Spark execution audit metrics.

---

## 8. Downstream ML & RAG Boundary

Machine Learning (K-Means clustering, anomaly detection, price forecasting), RAG assistants, and Streamlit are **strictly downstream consumer layers**. The core data engineering pipeline functions completely independently. See [docs/ML_HANDOFF.md](docs/ML_HANDOFF.md) for full details.
