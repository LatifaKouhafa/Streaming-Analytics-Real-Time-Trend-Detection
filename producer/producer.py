"""
Kafka Producer: GDELT Articles Ingestion
---------------------------------------

This script continuously fetches recent news articles from the GDELT Doc API,
filters them by sustainability-related keywords, and publishes them to a Kafka
topic in JSON format.

Architecture role:
    GDELT API  -->  Kafka Producer  -->  Kafka Topic ("articles")

The produced messages are later consumed by Spark Structured Streaming
for language detection, sentiment analysis, and storage.
"""

import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer


# -------------------------------------------------------------------
# Kafka Producer configuration
# -------------------------------------------------------------------
# The producer connects to Kafka running on the host machine.
# (Inside Docker, Spark will use kafka:29092 instead.)
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


# -------------------------------------------------------------------
# Global configuration
# -------------------------------------------------------------------
TOPIC = "articles"                 # Kafka topic where articles are published
POLL_SECONDS = 10                  # Delay between two API polling cycles
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# GDELT full-text search query
QUERY = '(sustainability OR "renewable energy" OR "carbon emissions")'

# In-memory set to avoid re-sending the same article multiple times
SEEN = set()


# -------------------------------------------------------------------
# Kafka delivery callback
# -------------------------------------------------------------------
def delivery_report(err, msg):
    """
    Callback function executed by Kafka after message delivery.

    Parameters:
        err (KafkaError): Delivery error (if any)
        msg (Message): Kafka message metadata
    """
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Delivered to {msg.topic()} "
            f"(partition={msg.partition()}, offset={msg.offset()})"
        )


print("🟢 Producer is running (GDELT -> Kafka)")


# -------------------------------------------------------------------
# Main polling loop
# -------------------------------------------------------------------
while True:
    try:
        # ------------------------------------------------------------
        # Step 1: Fetch recent articles from the GDELT Doc API
        # ------------------------------------------------------------
        response = requests.get(
            GDELT_URL,
            params={
                "query": QUERY,
                "mode": "ArtList",
                "format": "json",
                "sort": "DateDesc",
                "maxrecords": 200,
                "timespan": "24h"
            },
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )

        # Handle API errors gracefully
        if response.status_code != 200:
            print("❌ GDELT API error:", response.status_code, response.text[:300])
            time.sleep(POLL_SECONDS)
            continue

        data = response.json()
        articles = data.get("articles", []) or []


        # ------------------------------------------------------------
        # Step 2: Process and publish each article
        # ------------------------------------------------------------
        for article in articles:
            url = article.get("url")

            # Skip invalid or already-processed articles
            if not url or url in SEEN:
                continue

            # Extract relevant fields with safe fallbacks
            title = article.get("title") or article.get("description") or "Untitled"
            source = article.get("sourceCommonName") or article.get("domain") or "unknown"
            country = article.get("sourceCountry") or "unknown"

            # Build a clean, structured event payload
            event = {
                "article_id": url,
                "theme": "sustainability_energy_climate",
                "source": source,
                "source_country": country,
                "title": title,
                "url": url,
                "published_at": article.get("seendate")
                                or datetime.now(timezone.utc).isoformat(),
                "provider": "gdelt_doc_api"
            }

            # Serialize the event as UTF-8 encoded JSON
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")

            # --------------------------------------------------------
            # Step 3: Send the message to Kafka
            # --------------------------------------------------------
            producer.produce(
                topic=TOPIC,
                value=payload,
                callback=delivery_report
            )

            # Serve delivery callbacks without blocking
            producer.poll(0)

            # Mark article as processed
            SEEN.add(url)

            print(f"📰 article -> {title[:80]}...")


        # Ensure all buffered messages are delivered before sleeping
        producer.flush()

    except Exception as e:
        # Catch unexpected runtime errors to keep the producer alive
        print("❌ Producer error:", e)

    # ------------------------------------------------------------
    # Step 4: Wait before the next polling cycle
    # ------------------------------------------------------------
    time.sleep(POLL_SECONDS)
