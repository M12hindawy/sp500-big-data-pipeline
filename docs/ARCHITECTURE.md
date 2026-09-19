# S&P 500 Stocks Pipeline Architecture

A single-machine, production-grade Big Data pipeline processing 25 years of S&P 500 historical stock market data using Apache Spark, Apache Kafka, MinIO, PostgreSQL, and Apache Airflow.

---

## 1. High-Level Data Lineage

```
                          KAGGLE S&P 500 DATASET
                       (sp500_stocks.csv, sp500_companies.csv)
                                     |
                                     v
                          MINIO BRONZE (Object Lake)
                                     |
             +-----------------------+-----------------------+
             |                                               |
             v                                               v
        SPARK BATCH                                   PYTHON PRODUCER
             |                                               |
             |                                               v
             |                                             KAFKA
             |                                               |
             |                                               v
             |                                     SPARK STRUCTURED STREAMING
             |                                               |
             +-----------------------+-----------------------+
                                     |
                                     v
                              VALIDATION & CLEANING
                                     |
                                     v
                          FINANCIAL TRANSFORMATIONS
                                     |
                                     v
                         AGGREGATIONS & SPARK SQL
                                     |
                     +---------------+---------------+
                     |                               |
                     v                               v
                MINIO SILVER                    MINIO GOLD
             (Partitioned Parquet)           (Parquet Aggregates)
                                                     |
                                                     v
                                             POSTGRESQL SERVING
                                                     |
                                                     v
                                         POWER BI / ANALYTICS VIEWS
```

---

## 2. Architectural Layer Boundaries

### A. Core Data Pipeline Layer (The implemented boundary)
The core engineering pipeline encompasses:
- Kaggle historical data ingestion into MinIO Bronze
- Historical Kafka replay producer
- Spark Batch & Spark Structured Streaming processing engines
- MinIO Silver & Gold Parquet lakehouse storage
- PostgreSQL analytical serving layer with Power BI SQL views
- Apache Airflow batch workflow orchestration and data-quality gates

### B. Downstream Consumers (Strictly Separated)
- **Power BI / Analytics**: Connects to PostgreSQL analytics tables and views (`daily_market_metrics`, `vw_latest_stock_metrics`, `vw_top_volume`, etc.). It is a visualization client, not a core processing service.
- **Machine Learning**: Consumes the feature-engineered Gold dataset (`gold/daily_market_metrics`). ML model training (K-Means, anomaly detection, price forecasting) is an independent downstream phase described in [ML_HANDOFF.md](ML_HANDOFF.md).
- **RAG & Vector Search**: Consumes metadata and Gold metrics via a documented schema interface. Vector databases and LLM frameworks are external to the core pipeline.
- **Streamlit**: Optional downstream web application reading PostgreSQL. It does not perform data ingestion or processing.

---

## 3. Streaming Time Model & Semantics

> [!IMPORTANT]
> **Replay Time Semantics Notice**:  
> The replay time is synthetic and exists only to simulate streaming behavior. It is not real market timestamp data.

The source dataset consists of **daily historical market observations**. To faithfully demonstrate event-time streaming without falsely claiming intraday tick data, two separate concepts are maintained:

1. **`trading_date` (Business Date)**:
   - Preserves the exact historical market date from Kaggle (e.g. `2020-01-02`).
   - Remains constant throughout Batch, Streaming, Silver, Gold, and PostgreSQL.
2. **`replay_time` (Synthetic Replay Timestamp)**:
   - Deterministic synthetic timestamp assigned during Kafka replay:
     $$\text{replay\_time} = \text{REPLAY\_START\_TIME} + (\text{record\_index} \times \text{REPLAY\_INTERVAL\_SECONDS})$$
   - Never uses wall-clock time (`datetime.now()`).
3. **`event_time` (Spark Event-Time)**:
   - For Spark Structured Streaming, $\text{event\_time} = \text{replay\_time}$.
   - Watermarking is applied strictly to `event_time`: `withWatermark("event_time", "10 minutes")`.
   - Windows are aggregated as a **5-Minute Replay Simulation Window**: `window("event_time", "5 minutes")`.
   - Records sharing the same historical `trading_date` are distributed across synthetic replay windows rather than collapsing to midnight.

---

## 4. Storage Architecture (Bronze / Silver / Gold)

| Layer | Path | Format | Partitioning | Contents |
|---|---|---|---|---|
| **Bronze** | `s3a://stock-data/bronze/` | Raw CSV | Unpartitioned | Source-preserving `sp500_stocks.csv` and `sp500_companies.csv` |
| **Silver** | `s3a://stock-data/silver/stocks/` | Parquet | `year`, `month` | Cleaned, validated, deduplicated daily stock observations |
| **Gold** | `s3a://stock-data/gold/daily_market_metrics/` | Parquet | Unpartitioned | Feature-engineered analytical tables ready for BI & ML |
| **Gold Streaming** | `s3a://stock-data/gold/streaming_market_metrics/` | Parquet | Append | 5-minute replay window aggregations |
| **Quarantine** | `s3a://stock-data/quarantine/` | Parquet | Append | Invalid/rejected records with rejection reasons |
| **Checkpoints** | `s3a://stock-data/checkpoints/streaming/` | Metadata | Internal Spark | Structured Streaming state store & offset commits |
