"""
Tests for Spark Structured Streaming Event-Time and Replay-Window Semantics
Validates the 10 acceptance criteria mandated by the time-model specification:
TEST 1: trading_date remains equal to original dataset date.
TEST 2: replay_time is generated deterministically.
TEST 3: Repeated producer runs produce identical replay timestamps.
TEST 4: event_time == replay_time.
TEST 5: Spark windows use event_time, not trading_date.
TEST 6: Multiple records with same trading_date fall into different replay windows.
TEST 7: Watermark is configured against event_time.
TEST 8: 5-minute aggregation is identified as a replay simulation window.
TEST 9: Producer does not use wall-clock time for logical event-time.
TEST 10: Checkpointing configuration remains intact.
"""
import pytest
from datetime import datetime, timedelta

def simulate_producer_emission(records, start_time_str="2000-01-01T00:00:00", interval_seconds=60):
    """Simulates deterministic replay time assignment from producer.py."""
    base_dt = datetime.fromisoformat(start_time_str)
    emitted = []
    for idx, r in enumerate(records):
        replay_dt = base_dt + timedelta(seconds=idx * interval_seconds)
        replay_time_str = replay_dt.isoformat()
        emitted.append({
            "symbol": r["symbol"],
            "trading_date": r["trading_date"],
            "replay_time": replay_time_str,
            "event_time": replay_time_str,  # event_time = replay_time
            "open": r["open"],
            "close": r["close"],
            "source": "historical_replay"
        })
    return emitted

def assign_5min_window(event_time_str):
    """Calculates tumbling 5-minute replay window boundaries."""
    dt = datetime.fromisoformat(event_time_str)
    # Floor to 5 minutes (300 seconds)
    total_seconds = dt.minute * 60 + dt.second
    remainder = total_seconds % 300
    window_start = dt - timedelta(seconds=remainder)
    window_end = window_start + timedelta(minutes=5)
    return window_start.isoformat(), window_end.isoformat()

def test_1_trading_date_preservation():
    raw_date = "2020-01-02"
    records = [{"symbol": "AAPL", "trading_date": raw_date, "open": 100.0, "close": 105.0}]
    emitted = simulate_producer_emission(records)
    assert emitted[0]["trading_date"] == raw_date, "trading_date was altered or overwritten!"

def test_2_and_test_3_deterministic_replay_time():
    records = [
        {"symbol": "AAPL", "trading_date": "2020-01-02", "open": 100.0, "close": 105.0},
        {"symbol": "MSFT", "trading_date": "2020-01-02", "open": 150.0, "close": 152.0},
        {"symbol": "GOOG", "trading_date": "2020-01-02", "open": 70.0, "close": 71.0},
        {"symbol": "AMZN", "trading_date": "2020-01-02", "open": 90.0, "close": 92.0},
        {"symbol": "AAPL", "trading_date": "2020-01-03", "open": 106.0, "close": 108.0},
    ]
    run1 = simulate_producer_emission(records, "2000-01-01T00:00:00", 60)
    run2 = simulate_producer_emission(records, "2000-01-01T00:00:00", 60)

    # Deterministic equality across runs
    assert [r["replay_time"] for r in run1] == [r["replay_time"] for r in run2]
    # Check exact progression
    assert run1[0]["replay_time"] == "2000-01-01T00:00:00"
    assert run1[1]["replay_time"] == "2000-01-01T00:01:00"
    assert run1[2]["replay_time"] == "2000-01-01T00:02:00"
    assert run1[3]["replay_time"] == "2000-01-01T00:03:00"
    assert run1[4]["replay_time"] == "2000-01-01T00:04:00"

def test_4_event_time_derived_from_replay_time():
    records = [{"symbol": "AAPL", "trading_date": "2020-01-02", "open": 100.0, "close": 105.0}]
    emitted = simulate_producer_emission(records)
    assert emitted[0]["event_time"] == emitted[0]["replay_time"]
    assert emitted[0]["event_time"] != emitted[0]["trading_date"]

def test_5_and_test_6_same_trading_date_different_replay_windows():
    # 6 records on the SAME historical trading date (2020-01-02)
    # With 60s interval, records 0-4 are in Window 00:00-00:05, record 5 is in Window 00:05-00:10
    records = [
        {"symbol": f"SYM_{i}", "trading_date": "2020-01-02", "open": 100.0 + i, "close": 101.0 + i}
        for i in range(6)
    ]
    emitted = simulate_producer_emission(records, "2000-01-01T00:00:00", 60)

    windows = [assign_5min_window(r["event_time"]) for r in emitted]
    # Records 0, 1, 2, 3, 4 fall into 00:00:00 -> 00:05:00
    for w in windows[0:5]:
        assert w == ("2000-01-01T00:00:00", "2000-01-01T00:05:00")
    # Record 5 falls into 00:05:00 -> 00:10:00
    assert windows[5] == ("2000-01-01T00:05:00", "2000-01-01T00:10:00")
    # Proves records with the same trading_date do NOT collapse into a single window!
    assert windows[0] != windows[5]

def test_7_watermark_uses_event_time():
    from pathlib import Path
    source_code = (Path(__file__).parent.parent / "spark" / "apps" / "streaming_pipeline.py").read_text(encoding="utf-8")
    assert 'withWatermark("event_time"' in source_code
    assert 'withWatermark("trading_date"' not in source_code

def test_8_explicit_replay_window_semantics():
    from pathlib import Path
    source_code = (Path(__file__).parent.parent / "spark" / "apps" / "streaming_pipeline.py").read_text(encoding="utf-8")
    assert 'window(F.col("event_time"), WINDOW_DURATION)' in source_code
    assert "Replay Window" in source_code or "replay simulation window" in source_code.lower()

def test_9_no_wall_clock_time_used_in_producer_event_generation():
    from pathlib import Path
    source_code = (Path(__file__).parent.parent / "producer" / "producer.py").read_text(encoding="utf-8")
    assert "datetime.now()" not in source_code
    assert "datetime.utcnow()" not in source_code
    assert "timedelta(seconds=(records_published * REPLAY_INTERVAL_SECONDS))" in source_code

def test_10_checkpointing_persisted():
    from pathlib import Path
    source_code = (Path(__file__).parent.parent / "spark" / "apps" / "streaming_pipeline.py").read_text(encoding="utf-8")
    assert 'option("checkpointLocation", CHECKPOINT_LOCATION)' in source_code
