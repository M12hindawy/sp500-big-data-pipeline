from kafka import KafkaConsumer
import json

c = KafkaConsumer(
    'stock-market-data',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    max_poll_records=8,
    consumer_timeout_ms=10000
)
msgs = list(c)
print(f'  Total messages in Kafka topic: {len(msgs)}')
print()
print('  Sample records (real historical data):')
print('  ' + '-'*60)
for m in msgs[:5]:
    d = json.loads(m.value)
    print(f"  {d['symbol']:6s}  {d['trading_date']}  close=${d['close']:8.2f}  vol={d['volume']:>12,}")
