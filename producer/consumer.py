import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "tweets_stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",   
    enable_auto_commit=True,
    group_id="demo-consumer-group", 
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("✅ Consumer started. Waiting for messages...")

for msg in consumer:
    print("📩 Received:", msg.value)
