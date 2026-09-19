"""
Historical Stock Replay Producer for S&P 500 Dataset
Replays daily historical OHLCV data to Kafka simulating a streaming event source.

TIME MODEL SEMANTICS:
- trading_date: Original historical business date from Kaggle (e.g. 2020-01-02).
- replay_time: Deterministic synthetic timestamp advancing by REPLAY_INTERVAL_SECONDS per record.
- event_time: Streaming event-time derived from replay_time (event_time = replay_time).
- source: "historical_replay"
"""
import os
import csv
import json
import time
import hashlib
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SP500HistoricalProducer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-market-data")
DATA_PATH = os.getenv("PRODUCER_DATA_PATH", "/data/sp500_stocks.csv")

PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "100"))  # Records/second (0 for unlimited)
PRODUCER_MAX_RECORDS = int(os.getenv("PRODUCER_MAX_RECORDS", "5000"))  # 0 for unlimited
PRODUCER_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "50"))

REPLAY_START_TIME_STR = os.getenv("REPLAY_START_TIME", "2000-01-01T00:00:00")
REPLAY_INTERVAL_SECONDS = float(os.getenv("REPLAY_INTERVAL_SECONDS", "1"))

def get_kafka_producer(max_retries=30, retry_interval=2):
    """Establishes resilient connection to Kafka broker."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} (Attempt {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
                batch_size=16384
            )
            logger.info("Successfully connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not available yet. Retrying in {retry_interval}s...")
            time.sleep(retry_interval)
        except Exception as e:
            logger.error(f"Unexpected connection error: {e}. Retrying in {retry_interval}s...")
            time.sleep(retry_interval)
    raise ConnectionError(f"Failed to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} after {max_retries} attempts.")

def parse_and_validate_row(row):
    """
    Validates source row and extracts standard fields.
    Returns normalized dict or None if invalid.
    """
    try:
        symbol = row.get("symbol", "").strip().upper()
        date_str = row.get("date", "").strip()
        if not symbol or not date_str:
            return None

        # Parse date to verify format
        datetime.strptime(date_str, "%Y-%m-%d")

        open_p = float(row.get("open", 0.0))
        high_p = float(row.get("high", 0.0))
        low_p = float(row.get("low", 0.0))
        close_p = float(row.get("close", 0.0))
        adj_close_p = float(row.get("adj_close", close_p))
        volume = int(float(row.get("volume", 0)))

        # Basic validity
        if high_p < low_p or volume < 0 or open_p <= 0 or close_p <= 0:
            return None

        return {
            "symbol": symbol,
            "trading_date": date_str,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "adj_close": adj_close_p,
            "volume": volume
        }
    except Exception:
        return None

def run_producer():
    logger.info("============================================================")
    logger.info("S&P 500 HISTORICAL STREAMING REPLAY PRODUCER STARTING")
    logger.info(f"Target Topic: {KAFKA_TOPIC}")
    logger.info(f"Data File: {DATA_PATH}")
    logger.info(f"Rate: {PRODUCER_RATE} records/sec (0 = max speed)")
    logger.info(f"Max Records: {PRODUCER_MAX_RECORDS} (0 = all)")
    logger.info(f"Replay Start Time: {REPLAY_START_TIME_STR}")
    logger.info(f"Replay Interval: {REPLAY_INTERVAL_SECONDS}s per record")
    logger.info("============================================================")

    if not os.path.exists(DATA_PATH):
        logger.error(f"Data file not found at {DATA_PATH}. Check DATASET_SETUP.md.")
        return

    producer = get_kafka_producer()
    
    try:
        replay_start = datetime.fromisoformat(REPLAY_START_TIME_STR)
    except Exception as e:
        logger.warning(f"Failed to parse REPLAY_START_TIME '{REPLAY_START_TIME_STR}': {e}. Defaulting to 2000-01-01T00:00:00")
        replay_start = datetime(2000, 1, 1, 0, 0, 0)

    records_read = 0
    records_published = 0
    records_rejected = 0
    kafka_errors = 0

    sleep_interval = (1.0 / PRODUCER_RATE) if PRODUCER_RATE > 0 else 0
    start_wall_clock = time.time()

    with open(DATA_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            records_read += 1
            if 0 < PRODUCER_MAX_RECORDS < records_read:
                logger.info(f"Reached maximum configured records limit ({PRODUCER_MAX_RECORDS}). Stopping replay.")
                break

            parsed = parse_and_validate_row(row)
            if not parsed:
                records_rejected += 1
                continue

            # Deterministic synthetic replay time progression
            # Logical time advances deterministically based on record emission index
            record_offset = timedelta(seconds=(records_published * REPLAY_INTERVAL_SECONDS))
            replay_dt = replay_start + record_offset
            replay_time_str = replay_dt.isoformat()
            event_time_str = replay_time_str  # event_time = replay_time

            # Deterministic unique event_id
            event_seed = f"{parsed['symbol']}_{parsed['trading_date']}_{records_published}_historical_replay"
            event_id = hashlib.sha256(event_seed.encode("utf-8")).hexdigest()[:16]

            event_payload = {
                "event_id": event_id,
                "symbol": parsed["symbol"],
                "trading_date": parsed["trading_date"],
                "replay_time": replay_time_str,
                "event_time": event_time_str,
                "open": parsed["open"],
                "high": parsed["high"],
                "low": parsed["low"],
                "close": parsed["close"],
                "adj_close": parsed["adj_close"],
                "volume": parsed["volume"],
                "source": "historical_replay"
            }

            try:
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=parsed["symbol"],
                    value=event_payload
                )
                records_published += 1
            except Exception as e:
                kafka_errors += 1
                logger.error(f"Error publishing record {event_id}: {e}")

            if records_published % 1000 == 0:
                producer.flush()
                elapsed = time.time() - start_wall_clock
                cur_rate = (records_published / elapsed) if elapsed > 0 else 0
                logger.info(
                    f"PROGRESS: READ={records_read} | PUBLISHED={records_published} | "
                    f"REJECTED={records_rejected} | ERRORS={kafka_errors} | RATE={cur_rate:.1f} rec/s | "
                    f"LAST_REPLAY_TIME={replay_time_str}"
                )

            if sleep_interval > 0:
                time.sleep(sleep_interval)

    producer.flush()
    producer.close()

    total_time = time.time() - start_wall_clock
    avg_rate = (records_published / total_time) if total_time > 0 else 0
    logger.info("============================================================")
    logger.info("PRODUCER REPLAY RUN COMPLETED")
    logger.info(f"TOTAL READ:        {records_read}")
    logger.info(f"TOTAL PUBLISHED:   {records_published}")
    logger.info(f"TOTAL REJECTED:    {records_rejected}")
    logger.info(f"KAFKA ERRORS:      {kafka_errors}")
    logger.info(f"TOTAL TIME:        {total_time:.2f} seconds")
    logger.info(f"AVG PUBLISH RATE:  {avg_rate:.1f} records/second")
    logger.info("============================================================")

if __name__ == "__main__":
    run_producer()
