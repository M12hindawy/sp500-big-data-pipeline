# S&P 500 Data Pipeline - Downstream ML & AI Handoff

This document defines the interface and data contracts for downstream teams building Machine Learning models, RAG assistants, and interactive analytics applications.

---

## 1. Architectural Boundary Principle

```
+-------------------------------------------------------------------+
|                     CORE DATA PIPELINE                            |
|  Kaggle -> MinIO Bronze -> Kafka/Batch -> Spark -> Silver/Gold     |
|                              -> PostgreSQL Serving Layer          |
+-------------------------------------------------------------------+
                                  |
                                  v
+---------------------------------+---------------------------------+
|                                 |                                 |
v                                 v                                 v
[DOWNSTREAM: ML / CLUSTERING]     [DOWNSTREAM: RAG ASSISTANT]      [DOWNSTREAM: BI / UI]
- K-Means Market Regimes          - Vector DB Retrieval             - Power BI Dashboards
- Volatility Anomaly Detection    - LLM Market Q&A Assistant        - Streamlit Explorer
- Multi-Day Price Forecasting
```

> [!IMPORTANT]
> **Pipeline Isolation**:
> - Machine learning model training, clustering, vector embeddings, and LLM orchestration are strictly downstream consumers.
> - The core data pipeline operates completely independently and must never be altered to accommodate model training.
> - Downstream ML and RAG models consume stable, versioned Gold Parquet tables or PostgreSQL analytics views.

---

## 2. ML Handoff Contract

### Primary Ingestion Endpoints
1. **High-Throughput Lakehouse Path**:
   - Location: `s3a://stock-data/gold/daily_market_metrics/`
   - Format: Apache Parquet
   - Partitioning: Unified analytical dataset with columns `symbol`, `trading_date`, `sector`, `sub_industry`.
2. **Relational / Micro-Serving Path**:
   - Database: `stock_analytics` in PostgreSQL
   - Table: `daily_market_metrics`
   - View: `vw_latest_stock_metrics`

### Recommended Feature Columns for ML
| Feature Name | Type | Scaling Recommendation | Potential Use Cases |
|---|---|---|---|
| `price_change_pct` | Float | StandardScaler / RobustScaler | Momentum classification, trend prediction |
| `intraday_range_pct` | Float | MinMaxScaler | Volatility clustering, anomaly detection |
| `dollar_volume` | Float | Log1p Transform (`log(1 + x)`) | Liquidity tiering, market cap weighting |
| `volume` | BigInt | RobustScaler | Volume breakout indicators |
| `typical_price` | Float | Normalized price relative to MA | Mean-reversion models |
| `sector` | String | One-Hot Encoding / Target Encoding | Sector-relative regime analysis |

### Potential ML Tasks
1. **Unsupervised Clustering (K-Means / PCA)**:
   - Group stocks into behavioral clusters based on daily volatility (`intraday_range_pct`) and traded liquidity (`dollar_volume`).
2. **Market Regime Detection**:
   - Classify market phases (bullish expansion, high-volatility contraction, range-bound consolidation).
3. **Price Anomaly Detection (Isolation Forests / Autoencoders)**:
   - Flag anomalous single-day moves exceeding statistical expectations given the stock's sector.

---

## 3. RAG & AI Assistant Boundary

A future RAG (Retrieval-Augmented Generation) layer can ingest:
1. `docs/DATA_DICTIONARY.md` and `docs/PIPELINE_CONTRACT.md` for text embeddings.
2. PostgreSQL tables (`daily_market_metrics`, `symbol_summary`) for structured SQL-query generation via LangChain/LlamaIndex.
3. Vector databases (e.g. Qdrant, Chroma) should run as separate standalone services outside the core docker compose stack.

---

## 4. Known Dataset Limitations

1. **Survivorship Bias**:
   The `sp500_companies.csv` and ticker list represent current constituents. Historic records do not account for firms that were delisted or removed from the S&P 500 index prior to today.
2. **Daily Observation Granularity**:
   The source data is daily OHLCV, not tick-by-tick real-time data. The streaming replay uses synthetic `replay_time` and 5-minute replay windows for simulation purposes.
