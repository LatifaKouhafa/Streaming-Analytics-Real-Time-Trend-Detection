import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "tweets_stream"

while True:
    keywords = ["data", "ai", "sport", "finance", "music"]

    
    message = {
        "text": "I love data engineering",
        "keyword": random.choice(keywords),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    producer.send(topic, message)
    print("Sent:", message)

    time.sleep(2)
