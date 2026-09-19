-- ============================================================
-- S&P 500 Data Pipeline Validation Queries
-- Automated and manual verification checks
-- ============================================================

-- Check 1: Null or Empty Keys in Gold
SELECT COUNT(*) AS null_keys_count
FROM daily_market_metrics
WHERE symbol IS NULL OR symbol = '' OR trading_date IS NULL;

-- Check 2: Invalid OHLC Relationships in Gold (Must return 0)
SELECT COUNT(*) AS invalid_ohlc_count
FROM daily_market_metrics
WHERE high_price < low_price 
   OR high_price < open_price 
   OR high_price < close_price
   OR low_price > open_price 
   OR low_price > close_price;

-- Check 3: Negative Volumes or Non-positive Prices (Must return 0)
SELECT COUNT(*) AS invalid_values_count
FROM daily_market_metrics
WHERE volume < 0 OR open_price <= 0 OR close_price <= 0;

-- Check 4: Duplicate Symbol + Trading Date in Gold (Must return 0 due to PK)
SELECT symbol, trading_date, COUNT(*) AS dup_count
FROM daily_market_metrics
GROUP BY symbol, trading_date
HAVING COUNT(*) > 1;

-- Check 5: Streaming Market Metrics Replay Window Verification
-- Ensures window_start and window_end reflect 5-minute replay durations
SELECT 
    symbol,
    window_start,
    window_end,
    EXTRACT(EPOCH FROM (window_end - window_start)) AS window_duration_seconds,
    first_price,
    last_price,
    record_count
FROM streaming_market_metrics
LIMIT 10;

-- Check 6: Pipeline Execution Status
SELECT * FROM pipeline_runs ORDER BY start_time DESC LIMIT 5;

-- Check 7: Data Quality Check Logs
SELECT * FROM data_quality_results ORDER BY created_at DESC LIMIT 10;
