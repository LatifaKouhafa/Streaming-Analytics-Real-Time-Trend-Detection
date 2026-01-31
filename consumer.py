import json
from confluent_kafka import Consumer, KafkaException

TOPIC = "articles"  

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "articles-consumer-en",
    "auto.offset.reset": "earliest"  # pour relire depuis le début
}

c = Consumer(conf)
c.subscribe([TOPIC])

print(f"🟢 Consumer listening on topic: {TOPIC}")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        raw = msg.value().decode("utf-8")
        event = json.loads(raw)

        print("✅ ARTICLE")
        print(" - title:", event.get("title") or event.get("text"))
        print(" - url:", event.get("url") or event.get("tweet_id"))
        print(" - theme:", event.get("theme") or event.get("keyword"))
        print("-" * 60)

except KeyboardInterrupt:
    print("🛑 Stopped by user")

finally:
    c.close()