import json
from kafka import KafkaConsumer
from datetime import datetime

consumer = KafkaConsumer(
    "tweets_stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",   
    enable_auto_commit=True,
    group_id="counter-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

current_minute = None
count = 0

print("✅ Counter started. Counting messages per minute...")

for msg in consumer:
    ts = msg.value["timestamp"]

    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if "Z" in ts else datetime.fromisoformat(ts)
    minute_key = dt.strftime("%Y-%m-%d %H:%M")

    if current_minute is None:
        current_minute = minute_key

    if minute_key != current_minute:
        print(f"🕐 Minute {current_minute} -> {count} messages")
        current_minute = minute_key
        count = 0

    count += 1
