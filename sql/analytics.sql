-- ============================================================
-- S&P 500 Analytics SQL Suite
-- Minimum required business intelligence queries
-- ============================================================

-- 1. Top Volume Stocks (Historical by total dollar volume)
SELECT 
    symbol,
    company,
    sector,
    SUM(volume) AS total_volume_shares,
    ROUND(SUM(dollar_volume) / 1e9, 2) AS total_dollar_volume_billions,
    ROUND(AVG(close_price), 2) AS avg_close_price
FROM daily_market_metrics
GROUP BY symbol, company, sector
ORDER BY total_dollar_volume_billions DESC
LIMIT 10;

-- 2. Largest Daily Gainers (Top single-day percentage jumps)
SELECT 
    trading_date,
    symbol,
    company,
    sector,
    open_price,
    close_price,
    price_change,
    price_change_pct
FROM daily_market_metrics
WHERE price_change_pct > 0
ORDER BY price_change_pct DESC
LIMIT 10;

-- 3. Largest Daily Losers (Top single-day percentage drops)
SELECT 
    trading_date,
    symbol,
    company,
    sector,
    open_price,
    close_price,
    price_change,
    price_change_pct
FROM daily_market_metrics
WHERE price_change_pct < 0
ORDER BY price_change_pct ASC
LIMIT 10;

-- 4. Highest Intraday Range (Most volatile single-day swings)
SELECT 
    trading_date,
    symbol,
    company,
    high_price,
    low_price,
    intraday_range,
    intraday_range_pct
FROM daily_market_metrics
ORDER BY intraday_range_pct DESC
LIMIT 10;

-- 5. Average Volume by Symbol
SELECT 
    symbol,
    company,
    ROUND(AVG(volume)) AS avg_daily_volume,
    COUNT(DISTINCT trading_date) AS days_traded
FROM daily_market_metrics
GROUP BY symbol, company
ORDER BY avg_daily_volume DESC;

-- 6. Average Return by Sector
SELECT 
    sector,
    COUNT(DISTINCT symbol) AS active_symbols,
    ROUND(AVG(price_change_pct), 4) AS avg_daily_return_pct,
    ROUND(AVG(intraday_range_pct), 4) AS avg_daily_volatility_pct,
    ROUND(SUM(dollar_volume) / 1e9, 2) AS sector_total_dollar_volume_b
FROM daily_market_metrics
GROUP BY sector
ORDER BY sector_total_dollar_volume_b DESC;

-- 7. Daily Market Summary (Aggregate market overview per day)
SELECT 
    trading_date,
    COUNT(DISTINCT symbol) AS symbols_traded,
    ROUND(AVG(close_price), 2) AS market_avg_close,
    ROUND(AVG(price_change_pct), 4) AS market_avg_change_pct,
    SUM(volume) AS total_market_shares,
    ROUND(SUM(dollar_volume) / 1e9, 2) AS total_market_dollar_volume_b
FROM daily_market_metrics
GROUP BY trading_date
ORDER BY trading_date DESC;

-- 8. Symbol Historical Summary (Lifetime high, low, return)
SELECT 
    symbol,
    company,
    MIN(trading_date) AS earliest_date,
    MAX(trading_date) AS latest_date,
    MIN(low_price) AS lifetime_low,
    MAX(high_price) AS lifetime_high,
    ROUND(AVG(close_price), 2) AS lifetime_avg_close
FROM daily_market_metrics
GROUP BY symbol, company
ORDER BY symbol ASC;
