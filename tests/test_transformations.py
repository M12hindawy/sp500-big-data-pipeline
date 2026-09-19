"""
Unit Tests for Financial Metric Calculations and Transformations
"""
import pytest

def calculate_metrics(open_p, high_p, low_p, close_p, volume):
    price_change = round(close_p - open_p, 4)
    price_change_pct = round(((close_p - open_p) / open_p) * 100.0, 4) if open_p > 0 else 0.0
    intraday_range = round(high_p - low_p, 4)
    intraday_range_pct = round(((high_p - low_p) / open_p) * 100.0, 4) if open_p > 0 else 0.0
    typical_price = round((high_p + low_p + close_p) / 3.0, 4)
    dollar_volume = round(close_p * volume, 2)
    return {
        "price_change": price_change,
        "price_change_pct": price_change_pct,
        "intraday_range": intraday_range,
        "intraday_range_pct": intraday_range_pct,
        "typical_price": typical_price,
        "dollar_volume": dollar_volume
    }

def test_price_change_calculations():
    # Normal case: gain
    metrics = calculate_metrics(open_p=100.0, high_p=110.0, low_p=95.0, close_p=105.0, volume=1000)
    assert metrics["price_change"] == 5.0
    assert metrics["price_change_pct"] == 5.0
    assert metrics["intraday_range"] == 15.0
    assert metrics["intraday_range_pct"] == 15.0
    assert metrics["typical_price"] == round((110.0 + 95.0 + 105.0) / 3.0, 4)
    assert metrics["dollar_volume"] == 105000.0

def test_loss_calculations():
    # Normal case: loss
    metrics = calculate_metrics(open_p=100.0, high_p=102.0, low_p=90.0, close_p=92.0, volume=500)
    assert metrics["price_change"] == -8.0
    assert metrics["price_change_pct"] == -8.0
    assert metrics["intraday_range"] == 12.0
    assert metrics["intraday_range_pct"] == 12.0

def test_zero_open_safe_division():
    metrics = calculate_metrics(open_p=0.0, high_p=10.0, low_p=0.0, close_p=5.0, volume=100)
    assert metrics["price_change_pct"] == 0.0
    assert metrics["intraday_range_pct"] == 0.0
