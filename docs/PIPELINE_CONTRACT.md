# S&P 500 Data Pipeline Contract

This document defines the strict structural, semantic, and operational interface contracts between each processing stage in the pipeline.

---

## 1. Source Contract (Kaggle Raw)

### Input Files
- `sp500_stocks.csv`: Daily stock observations.
- `sp500_companies.csv`: Company reference metadata.

### Logical Schema
```
sp500_stocks.csv:
  date:       STRING (YYYY-MM-DD)
  open:       FLOAT / DOUBLE (> 0)
  high:       FLOAT / DOUBLE (>= low, >= open, >= close)
  low:        FLOAT / DOUBLE (<= high, <= open, <= close)
  close:      FLOAT / DOUBLE (> 0)
  adj_close:  FLOAT / DOUBLE (> 0)
  volume:     INTEGER / BIGINT (>= 0)
  symbol:     STRING (Non-empty uppercase ticker)

sp500_companies.csv:
  symbol:       STRING (Primary Key)
  company:      STRING
  sector:       STRING
  sub_industry: STRING
  headquarters: STRING
  date_added:   STRING
  founded:      INTEGER
```

---

## 2. Kafka Event Contract

### Transport Protocol
- Topic: `stock-market-data`
- Partitions: 3
- Key: `symbol` (UTF-8 encoded string)
- Format: JSON (UTF-8 encoded)

### Canonical JSON Payload
```json
{
  "event_id": "8f3b2c1a4e5d6f70",
  "symbol": "AAPL",
  "trading_date": "2020-01-02",
  "replay_time": "2000-01-01T00:00:00",
  "event_time": "2000-01-01T00:00:00",
  "open": 75.0875,
  "high": 75.1500,
  "low": 73.7975,
  "close": 75.0875,
  "adj_close": 73.0594,
  "volume": 135480400,
  "source": "historical_replay"
}
```

### Invariants
1. `event_id` is a deterministic 16-character hexadecimal hash.
2. `trading_date` is the Kaggle historical business date.
3. `replay_time` and `event_time` are synthetic UTC timestamps advancing deterministically per emitted record.
4. Wall-clock system time is **never** used as event-time.

---

## 3. Silver Contract (Cleaned & Validated)

- Path: `s3a://stock-data/silver/stocks/`
- Format: Parquet (Snappy compressed)
- Partitioning: `year: INT`, `month: INT`
- Grain: One record per `(symbol, trading_date)`
- Guarantee: Deduplicated, zero null business keys, zero OHLC violations.

---

## 4. Gold Contract (Aggregates & Features)

- Path: `s3a://stock-data/gold/daily_market_metrics/`
- Format: Parquet
- Grain: One record per `(symbol, trading_date)`
- Additional Fields:
  `price_change`, `price_change_pct`, `intraday_range`, `intraday_range_pct`, `typical_price`, `dollar_volume`, `company`, `sector`, `sub_industry`.

---

## 5. PostgreSQL Serving Contract

### Database: `stock_analytics`

#### Table: `daily_market_metrics`
- Primary Key: `(symbol, trading_date)`
- Write Pattern: Idempotent upsert via staging table (`ON CONFLICT (symbol, trading_date) DO UPDATE`)
- Consumers: Power BI dashboards, SQL ad-hoc queries, ML feature extraction.

#### Table: `streaming_market_metrics`
- Unique Constraint: `(symbol, window_start, window_end)`
- Aggregation Granularity: 5-minute replay simulation window on `event_time`.
- Write Pattern: Microbatch idempotent upsert via `foreachBatch`.
