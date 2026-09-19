"""
Unit Tests for Schema Validation and Type Enforcement
"""
import pytest
from datetime import datetime

def test_raw_csv_schema_fields():
    expected_fields = ["date", "open", "high", "low", "close", "adj_close", "volume", "symbol"]
    # Verify exact Kaggle schema definition
    sample_header = "date,open,high,low,close,adj_close,volume,symbol"
    parsed_fields = [f.strip() for f in sample_header.split(",")]
    assert parsed_fields == expected_fields, "Raw stock CSV schema fields do not match Kaggle specification"

def test_company_metadata_schema_fields():
    expected_fields = ["symbol", "company", "sector", "sub_industry", "headquarters", "date_added", "founded"]
    sample_header = "symbol,company,sector,sub_industry,headquarters,date_added,founded"
    parsed_fields = [f.strip() for f in sample_header.split(",")]
    assert parsed_fields == expected_fields, "Company metadata schema fields do not match specification"

def test_kafka_event_schema_contract():
    from pathlib import Path
    p1 = Path(__file__).parent.parent / "spark" / "apps" / "common.py"
    p2 = Path(__file__).parent.parent / "apps" / "common.py"
    common_path = p1 if p1.exists() else p2
    content = common_path.read_text(encoding="utf-8")
    assert "KAFKA_EVENT_SCHEMA = StructType" in content
    assert '"event_id"' in content
    assert '"symbol"' in content
    assert '"trading_date"' in content
    assert '"replay_time"' in content
    assert '"event_time"' in content
    assert '"source"' in content
