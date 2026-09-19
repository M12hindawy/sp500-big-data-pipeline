"""
Verification script for deterministic sample event emission.
"""
import csv
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data" / "sp500_stocks.csv"

def test_emission():
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        base_dt = datetime.fromisoformat("2000-01-01T00:00:00")
        for i, row in enumerate(reader):
            if i >= 5:
                break
            replay_dt = base_dt + timedelta(seconds=i * 60)
            rt = replay_dt.isoformat()
            seed = f"{row['symbol']}_{row['date']}_{i}_historical_replay"
            eid = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
            payload = {
                "event_id": eid,
                "symbol": row["symbol"],
                "trading_date": row["date"],
                "replay_time": rt,
                "event_time": rt,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "adj_close": float(row["adj_close"]),
                "volume": int(row["volume"]),
                "source": "historical_replay"
            }
            print(f"Record {i + 1}: {json.dumps(payload)}")

if __name__ == "__main__":
    test_emission()
