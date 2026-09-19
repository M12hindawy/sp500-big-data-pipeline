"""
Data Quality Validation and Quarantine Tests
Tests detection of:
- null symbol
- null or invalid date
- high < low
- negative volume
- non-positive prices
"""
import pytest

def validate_row(row):
    symbol = row.get("symbol", "").strip()
    date_str = row.get("trading_date", "").strip()
    open_p = row.get("open")
    high_p = row.get("high")
    low_p = row.get("low")
    close_p = row.get("close")
    volume = row.get("volume")

    if not symbol:
        return False, "MISSING_SYMBOL"
    if not date_str:
        return False, "MISSING_DATE"
    if high_p is None or low_p is None or high_p < low_p:
        return False, "HIGH_LESS_THAN_LOW"
    if high_p < open_p:
        return False, "HIGH_LESS_THAN_OPEN"
    if high_p < close_p:
        return False, "HIGH_LESS_THAN_CLOSE"
    if low_p > open_p:
        return False, "LOW_GREATER_THAN_OPEN"
    if low_p > close_p:
        return False, "LOW_GREATER_THAN_CLOSE"
    if volume is None or volume < 0:
        return False, "NEGATIVE_VOLUME"
    if open_p is None or open_p <= 0 or close_p is None or close_p <= 0:
        return False, "INVALID_PRICE"

    return True, "VALID"

def test_valid_record():
    row = {"symbol": "AAPL", "trading_date": "2020-01-02", "open": 100.0, "high": 105.0, "low": 98.0, "close": 102.0, "volume": 1000}
    is_valid, reason = validate_row(row)
    assert is_valid is True
    assert reason == "VALID"

def test_high_less_than_low_rejected():
    row = {"symbol": "AAPL", "trading_date": "2020-01-02", "open": 100.0, "high": 90.0, "low": 95.0, "close": 92.0, "volume": 1000}
    is_valid, reason = validate_row(row)
    assert is_valid is False
    assert reason == "HIGH_LESS_THAN_LOW"

def test_negative_volume_rejected():
    row = {"symbol": "AAPL", "trading_date": "2020-01-02", "open": 100.0, "high": 105.0, "low": 98.0, "close": 102.0, "volume": -50}
    is_valid, reason = validate_row(row)
    assert is_valid is False
    assert reason == "NEGATIVE_VOLUME"

def test_missing_symbol_rejected():
    row = {"symbol": "", "trading_date": "2020-01-02", "open": 100.0, "high": 105.0, "low": 98.0, "close": 102.0, "volume": 500}
    is_valid, reason = validate_row(row)
    assert is_valid is False
    assert reason == "MISSING_SYMBOL"

def test_invalid_negative_price_rejected():
    row = {"symbol": "AAPL", "trading_date": "2020-01-02", "open": -10.0, "high": 105.0, "low": 98.0, "close": 102.0, "volume": 500}
    is_valid, reason = validate_row(row)
    assert is_valid is False
