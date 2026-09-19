# S&P 500 Data Pipeline - Data Dictionary

> [!IMPORTANT]
> **Mandatory Notice on Replay Time Semantics**:  
> The replay time is synthetic and exists only to simulate streaming behavior. It is not real market timestamp data.

---

## 1. Time Semantics & Dual-Time Fields

### `trading_date`
- **Type**: `DATE`
- **Source**: Kaggle `sp500_stocks.csv` (`date` column)
- **Meaning**: Original historical trading date on which the stock market session occurred.
- **Nullable**: No (Strict validation requirement)
- **Transformation**: Parsed from string `YYYY-MM-DD` to DateType.
- **Unit**: Calendar Date
- **Timestamp Semantics**: Historical market observation date. Preserved unmolested across Batch, Streaming, Silver, Gold, and PostgreSQL.

### `replay_time`
- **Type**: `TIMESTAMP`
- **Source**: Python Historical Replay Producer (`producer.py`)
- **Meaning**: Synthetic timestamp assigned during historical replay to simulate streaming event progression.
- **Nullable**: No
- **Transformation**: Calculated deterministically via `REPLAY_START_TIME + (record_index * REPLAY_INTERVAL_SECONDS)`. *Never generated using system clock or wall-clock `datetime.now()`.*
- **Unit**: UTC Timestamp
- **Timestamp Semantics**: Simulated streaming ingestion timeline.

### `event_time`
- **Type**: `TIMESTAMP`
- **Source**: Derived in Kafka event / Spark Structured Streaming
- **Meaning**: Streaming event-time used by Spark Structured Streaming for watermarking and window aggregations. Derived directly from `replay_time` (`event_time = replay_time`).
- **Nullable**: No
- **Transformation**: Cast from `replay_time` string to TimestampType.
- **Unit**: UTC Timestamp
- **Timestamp Semantics**: **event_time is NOT the original market timestamp.** It represents the synthetic replay progression clock for streaming simulation.

---

## 2. Core Stock Market Observation Fields

### `symbol`
- **Type**: `VARCHAR(16)` / `StringType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Stock ticker symbol identifying the company (e.g., `AAPL`, `MSFT`).
- **Nullable**: No (Natural business key)
- **Transformation**: Trimmed and converted to uppercase (`UPPER(TRIM(symbol))`).

### `open_price` (`open`)
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Price of the stock at the opening of regular market trading hours.
- **Nullable**: No
- **Transformation**: Cast to double/numeric. Validated: `open > 0` and `low <= open <= high`.
- **Unit**: USD ($)

### `high_price` (`high`)
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Highest price traded during the daily market session.
- **Nullable**: No
- **Transformation**: Validated: `high >= low`, `high >= open`, `high >= close`.
- **Unit**: USD ($)

### `low_price` (`low`)
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Lowest price traded during the daily market session.
- **Nullable**: No
- **Transformation**: Validated: `low <= high`, `low <= open`, `low <= close`.
- **Unit**: USD ($)

### `close_price` (`close`)
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Final settling price at market close.
- **Nullable**: No
- **Transformation**: Validated: `close > 0` and `low <= close <= high`.
- **Unit**: USD ($)

### `adj_close_price` (`adj_close`)
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Closing price adjusted for stock splits, stock dividends, and corporate distributions.
- **Nullable**: No
- **Unit**: USD ($)

### `volume`
- **Type**: `BIGINT` / `LongType`
- **Source**: Kaggle `sp500_stocks.csv`
- **Meaning**: Total number of shares traded during the trading day.
- **Nullable**: No
- **Transformation**: Validated: `volume >= 0`.
- **Unit**: Share Count

---

## 3. Financial Derived Features (Pipeline Level)

### `price_change`
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Derived (`close - open`)
- **Meaning**: Absolute intraday price difference between close and open.
- **Nullable**: No
- **Unit**: USD ($)

### `price_change_pct`
- **Type**: `NUMERIC(8, 4)` / `DoubleType`
- **Source**: Derived (`((close - open) / open) * 100.0`)
- **Meaning**: Percentage intraday price return with safe division guard.
- **Nullable**: No
- **Unit**: Percent (%)

### `intraday_range`
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Derived (`high - low`)
- **Meaning**: Absolute spread between high and low daily prices.
- **Nullable**: No
- **Unit**: USD ($)

### `intraday_range_pct`
- **Type**: `NUMERIC(8, 4)` / `DoubleType`
- **Source**: Derived (`((high - low) / open) * 100.0`)
- **Meaning**: Relative volatility of daily price range as a percentage of open.
- **Nullable**: No
- **Unit**: Percent (%)

### `typical_price`
- **Type**: `NUMERIC(12, 4)` / `DoubleType`
- **Source**: Derived (`(high + low + close) / 3.0`)
- **Meaning**: Arithmetic average of daily high, low, and closing prices.
- **Nullable**: No
- **Unit**: USD ($)

### `dollar_volume`
- **Type**: `NUMERIC(18, 2)` / `DoubleType`
- **Source**: Derived (`close * volume`)
- **Meaning**: Total estimated liquidity traded in dollars.
- **Nullable**: No
- **Unit**: USD ($)

---

## 4. Company Metadata Fields (Joined)

### `company`
- **Type**: `VARCHAR(255)` / `StringType`
- **Source**: Kaggle `sp500_companies.csv`
- **Meaning**: Official registered company name.

### `sector`
- **Type**: `VARCHAR(100)` / `StringType`
- **Source**: Kaggle `sp500_companies.csv`
- **Meaning**: GICS macro sector classification (e.g., `Information Technology`, `Financials`).

### `sub_industry`
- **Type**: `VARCHAR(150)` / `StringType`
- **Source**: Kaggle `sp500_companies.csv`
- **Meaning**: GICS granular industry group.

---

## 5. Streaming Window Aggregation Fields

### `window_start` / `window_end`
- **Type**: `TIMESTAMP`
- **Meaning**: Beginning and end of the **5-Minute Replay Simulation Window** computed against `event_time`.
- **Notice**: Does NOT represent a real 5-minute market candle.

### `first_price` / `last_price`
- **Type**: `NUMERIC(12, 4)`
- **Meaning**: First open price and last closing price observed within the 5-minute replay window.

### `record_count`
- **Type**: `BIGINT`
- **Meaning**: Number of daily historical records replayed within that 5-minute replay window.
