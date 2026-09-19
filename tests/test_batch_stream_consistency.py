"""
Batch vs Streaming Metric Consistency Verification
Ensures that price_change, price_change_pct, intraday_range, and dollar_volume
have mathematically identical formulas across both batch and streaming pipelines.
"""
import pytest

def batch_metrics(open_p, high_p, low_p, close_p, volume):
    return {
        "price_change": round(close_p - open_p, 4),
        "price_change_pct": round(((close_p - open_p) / open_p) * 100.0, 4) if open_p > 0 else 0.0,
        "intraday_range": round(high_p - low_p, 4),
        "dollar_volume": round(close_p * volume, 2)
    }

def streaming_window_metrics(first_p, max_p, min_p, last_p, total_vol):
    return {
        "price_change": round(last_p - first_p, 4),
        "price_change_pct": round(((last_p - first_p) / first_p) * 100.0, 4) if first_p > 0 else 0.0,
        "intraday_range": round(max_p - min_p, 4),
        "dollar_volume": round(last_p * total_vol, 2)
    }

def test_single_observation_window_equivalence():
    """
    When a streaming window contains exactly 1 observation,
    its metrics must be strictly identical to the batch metrics for that record.
    """
    open_p, high_p, low_p, close_p, volume = 150.25, 155.80, 149.10, 154.30, 2500000

    b = batch_metrics(open_p, high_p, low_p, close_p, volume)
    s = streaming_window_metrics(first_p=open_p, max_p=high_p, min_p=low_p, last_p=close_p, total_vol=volume)

    assert b["price_change"] == s["price_change"]
    assert b["price_change_pct"] == s["price_change_pct"]
    assert b["intraday_range"] == s["intraday_range"]
    assert b["dollar_volume"] == s["dollar_volume"]
