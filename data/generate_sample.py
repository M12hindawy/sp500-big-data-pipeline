"""
Generates a deterministic verification sample of sp500_stocks.csv and sp500_companies.csv
strictly following the Kaggle dataset schema:
sp500_stocks.csv: date, open, high, low, close, adj_close, volume, symbol
sp500_companies.csv: symbol, company, sector, sub_industry, headquarters, date_added, founded
"""
import csv
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path(__file__).resolve().parent

COMPANIES = [
    ("AAPL", "Apple Inc.", "Information Technology", "Technology Hardware, Storage & Peripherals", "Cupertino, California", "1982-11-30", 1976),
    ("MSFT", "Microsoft Corporation", "Information Technology", "Systems Software", "Redmond, Washington", "1994-06-01", 1975),
    ("GOOGL", "Alphabet Inc. (Class A)", "Communication Services", "Interactive Media & Services", "Mountain View, California", "2006-04-03", 1998),
    ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", "Broadline Retail", "Seattle, Washington", "2005-11-18", 1994),
    ("NVDA", "Nvidia Corporation", "Information Technology", "Semiconductors", "Santa Clara, California", "2001-11-30", 1993),
    ("JPM", "JPMorgan Chase & Co.", "Financials", "Diversified Banks", "New York City, New York", "1975-06-30", 1799),
    ("V", "Visa Inc.", "Financials", "Transaction & Payment Processing Services", "San Francisco, California", "2009-12-21", 1958),
    ("PG", "Procter & Gamble Company", "Consumer Staples", "Household Products", "Cincinnati, Ohio", "1957-03-04", 1837),
    ("JNJ", "Johnson & Johnson", "Health Care", "Pharmaceuticals", "New Brunswick, New Jersey", "1973-06-30", 1886),
    ("XOM", "Exxon Mobil Corporation", "Energy", "Integrated Oil & Gas", "Spring, Texas", "1957-03-04", 1870)
]

def generate_sample_data(num_days=100):
    stocks_file = DATA_DIR / "sp500_stocks.csv"
    companies_file = DATA_DIR / "sp500_companies.csv"

    print(f"Generating deterministic company metadata -> {companies_file}")
    with open(companies_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "company", "sector", "sub_industry", "headquarters", "date_added", "founded"])
        for comp in COMPANIES:
            writer.writerow(comp)

    print(f"Generating deterministic historical stocks observations -> {stocks_file}")
    start_date = datetime(2020, 1, 2)
    base_prices = {
        "AAPL": 75.0, "MSFT": 160.0, "GOOGL": 68.0, "AMZN": 95.0, "NVDA": 59.0,
        "JPM": 138.0, "V": 190.0, "PG": 123.0, "JNJ": 145.0, "XOM": 70.0
    }

    records_count = 0
    with open(stocks_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "open", "high", "low", "close", "adj_close", "volume", "symbol"])
        
        current_date = start_date
        day_idx = 0
        while day_idx < num_days:
            # Skip weekends (market closed)
            if current_date.weekday() < 5:
                for comp in COMPANIES:
                    sym = comp[0]
                    base = base_prices[sym] + (day_idx * 0.25)
                    # Create realistic OHLC
                    open_p = round(base, 2)
                    high_p = round(base + 2.5, 2)
                    low_p = round(base - 1.8, 2)
                    close_p = round(base + 0.9, 2)
                    adj_close_p = close_p
                    volume = int(1000000 + (day_idx * 5000) + (len(sym) * 20000))
                    date_str = current_date.strftime("%Y-%m-%d")
                    writer.writerow([date_str, open_p, high_p, low_p, close_p, adj_close_p, volume, sym])
                    records_count += 1
                day_idx += 1
            current_date += timedelta(days=1)

    print(f"Successfully generated {records_count} stock observations across {num_days} trading days.")

if __name__ == "__main__":
    generate_sample_data(num_days=60)
