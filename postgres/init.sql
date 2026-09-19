-- ============================================================
-- S&P 500 Stock Market Data Pipeline - PostgreSQL Serving Layer
-- Database: stock_analytics
-- ============================================================

-- 1. Pipeline Runs Audit Table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    input_rows BIGINT DEFAULT 0,
    output_rows BIGINT DEFAULT 0,
    rejected_rows BIGINT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Data Quality Results Table
CREATE TABLE IF NOT EXISTS data_quality_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    dataset VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    expected VARCHAR(100),
    actual VARCHAR(100),
    status VARCHAR(20) NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Gold Daily Market Metrics Table (Batch Output)
CREATE TABLE IF NOT EXISTS daily_market_metrics (
    trading_date DATE NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    company VARCHAR(255) DEFAULT 'Unknown',
    sector VARCHAR(100) DEFAULT 'Unknown',
    sub_industry VARCHAR(150) DEFAULT 'Unknown',
    open_price NUMERIC(12, 4) NOT NULL,
    high_price NUMERIC(12, 4) NOT NULL,
    low_price NUMERIC(12, 4) NOT NULL,
    close_price NUMERIC(12, 4) NOT NULL,
    adj_close_price NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    price_change NUMERIC(12, 4),
    price_change_pct NUMERIC(8, 4),
    intraday_range NUMERIC(12, 4),
    intraday_range_pct NUMERIC(8, 4),
    typical_price NUMERIC(12, 4),
    dollar_volume NUMERIC(18, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_daily_market_metrics PRIMARY KEY (symbol, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_dmm_date ON daily_market_metrics(trading_date);
CREATE INDEX IF NOT EXISTS idx_dmm_sector ON daily_market_metrics(sector);
CREATE INDEX IF NOT EXISTS idx_dmm_processed ON daily_market_metrics(processed_at);

-- Staging table for daily_market_metrics batch upserts
CREATE TABLE IF NOT EXISTS stg_daily_market_metrics (
    trading_date DATE,
    symbol VARCHAR(16),
    company VARCHAR(255),
    sector VARCHAR(100),
    sub_industry VARCHAR(150),
    open_price NUMERIC(12, 4),
    high_price NUMERIC(12, 4),
    low_price NUMERIC(12, 4),
    close_price NUMERIC(12, 4),
    adj_close_price NUMERIC(12, 4),
    volume BIGINT,
    price_change NUMERIC(12, 4),
    price_change_pct NUMERIC(8, 4),
    intraday_range NUMERIC(12, 4),
    intraday_range_pct NUMERIC(8, 4),
    typical_price NUMERIC(12, 4),
    dollar_volume NUMERIC(18, 2),
    processed_at TIMESTAMP
);

-- 4. Streaming Market Metrics Table (Spark Structured Streaming Output)
-- Aggregated over synthetic 5-minute replay simulation windows
CREATE TABLE IF NOT EXISTS streaming_market_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    first_price NUMERIC(12, 4),
    max_price NUMERIC(12, 4),
    min_price NUMERIC(12, 4),
    last_price NUMERIC(12, 4),
    total_volume BIGINT,
    average_close NUMERIC(12, 4),
    price_change_pct NUMERIC(8, 4),
    average_intraday_range NUMERIC(12, 4),
    record_count BIGINT,
    source VARCHAR(64) DEFAULT 'historical_replay',
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_symbol_window UNIQUE (symbol, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_smm_symbol ON streaming_market_metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_smm_window_start ON streaming_market_metrics(window_start);
CREATE INDEX IF NOT EXISTS idx_smm_processed ON streaming_market_metrics(processed_at);

-- Staging table for streaming_market_metrics microbatch upserts
CREATE TABLE IF NOT EXISTS stg_streaming_market_metrics (
    symbol VARCHAR(16),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    first_price NUMERIC(12, 4),
    max_price NUMERIC(12, 4),
    min_price NUMERIC(12, 4),
    last_price NUMERIC(12, 4),
    total_volume BIGINT,
    average_close NUMERIC(12, 4),
    price_change_pct NUMERIC(8, 4),
    average_intraday_range NUMERIC(12, 4),
    record_count BIGINT,
    source VARCHAR(64),
    processed_at TIMESTAMP
);

-- 5. Symbol Summary Aggregate Table
CREATE TABLE IF NOT EXISTS symbol_summary (
    symbol VARCHAR(16) PRIMARY KEY,
    company VARCHAR(255),
    sector VARCHAR(100),
    total_trading_days INT,
    min_price NUMERIC(12, 4),
    max_price NUMERIC(12, 4),
    avg_daily_volume BIGINT,
    latest_close_price NUMERIC(12, 4),
    last_trading_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- POWER BI & ANALYTICS VIEWS
-- ============================================================

-- View: Latest Stock Metrics
CREATE OR REPLACE VIEW vw_latest_stock_metrics AS
SELECT DISTINCT ON (symbol)
    symbol,
    company,
    sector,
    sub_industry,
    trading_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adj_close_price,
    volume,
    price_change,
    price_change_pct,
    intraday_range,
    dollar_volume
FROM daily_market_metrics
ORDER BY symbol, trading_date DESC;

-- View: Top 20 Stocks by Dollar Traded Volume
CREATE OR REPLACE VIEW vw_top_volume AS
SELECT 
    trading_date,
    symbol,
    company,
    sector,
    volume,
    dollar_volume,
    close_price
FROM daily_market_metrics
ORDER BY dollar_volume DESC
LIMIT 20;

-- View: Top Daily Gainers
CREATE OR REPLACE VIEW vw_top_gainers AS
SELECT 
    trading_date,
    symbol,
    company,
    sector,
    close_price,
    price_change,
    price_change_pct
FROM daily_market_metrics
WHERE price_change_pct > 0
ORDER BY price_change_pct DESC
LIMIT 20;

-- View: Top Daily Losers
CREATE OR REPLACE VIEW vw_top_losers AS
SELECT 
    trading_date,
    symbol,
    company,
    sector,
    close_price,
    price_change,
    price_change_pct
FROM daily_market_metrics
WHERE price_change_pct < 0
ORDER BY price_change_pct ASC
LIMIT 20;

-- View: Streaming Window Metrics Summary
CREATE OR REPLACE VIEW vw_streaming_metrics AS
SELECT 
    symbol,
    window_start,
    window_end,
    first_price,
    last_price,
    price_change_pct,
    total_volume,
    record_count,
    source
FROM streaming_market_metrics
ORDER BY window_start DESC;

-- View: Pipeline Run Health Summary
CREATE OR REPLACE VIEW vw_pipeline_health AS
SELECT 
    run_id,
    pipeline_name,
    status,
    input_rows,
    output_rows,
    rejected_rows,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (end_time - start_time)) AS duration_seconds,
    error_message
FROM pipeline_runs
ORDER BY start_time DESC;
