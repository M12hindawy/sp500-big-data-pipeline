# S&P 500 Stocks Dataset Setup Guide

This data pipeline requires the dataset:
**"S&P 500 Stocks: 25 Years of Data (Updated Daily)"**  
Kaggle URL: https://www.kaggle.com/datasets/darkmatternet/s-and-p-500-stocks-25-years-of-data-updated-daily  
License: CC0 (Public Domain)

---

## 1. Expected Files

Place the following files directly inside the `data/` folder:

1. `data/sp500_stocks.csv`
   - Daily OHLCV observations (3M+ rows, 2000 to present)
   - Columns: `date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`, `symbol`
2. `data/sp500_companies.csv`
   - Current S&P 500 constituent company metadata (~500 rows)
   - Columns: `symbol`, `company`, `sector`, `sub_industry`, `headquarters`, `date_added`, `founded`

---

## 2. Option A: Manual Download (Recommended)

1. Visit: https://www.kaggle.com/datasets/darkmatternet/s-and-p-500-stocks-25-years-of-data-updated-daily
2. Click **Download** (archive.zip ~ 65 MB).
3. Unzip the downloaded file and place `sp500_stocks.csv` and `sp500_companies.csv` inside:
   ```
   sp500-pipeline/data/
   ```

---

## 3. Option B: Automated Download via Kaggle API

If you have your Kaggle API key configured (`~/.kaggle/kaggle.json` or environment variables `KAGGLE_USERNAME` and `KAGGLE_KEY`):

```bash
python data/download_dataset.py
```

---

## 4. Option C: Generate Deterministic Verification Sample

To test the entire pipeline immediately before downloading the full multi-million row file:

```bash
python data/generate_sample.py
```
This generates a realistic, deterministic test set containing S&P 500 daily records (e.g. AAPL, MSFT, GOOG, AMZN, NVDA, TSLA) to validate the end-to-end flow.

---

## 5. Dataset Semantics & Streaming Replay Notice

- **Business Date**: Each row represents a DAILY market observation identified by `(symbol, trading_date)`.
- **Streaming Simulation**: Historical records are replayed through Kafka. The producer assigns a synthetic `replay_time` and `event_time` to simulate streaming progression.
- **Survivorship Notice**: The `sp500_companies.csv` file reflects current constituent metadata. Historical presence does not imply constituent membership across all 25 years.
