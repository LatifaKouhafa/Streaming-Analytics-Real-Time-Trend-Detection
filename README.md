# 📊 Streaming Analytics – Real-Time Trend Detection

##  Project Overview

This project demonstrates how to **analyze data in real time** in order to understand **what is happening as it happens**.

I built a **streaming data pipeline** that continuously receives messages (simulating tweets), processes them immediately, and displays real-time indicators to detect emerging trends.

The goal is to reproduce a **real business use case**, such as trend detection or real-time system monitoring.

---

## Business Problem

In domains like media monitoring, sustainability intelligence, or risk analysis, **batch processing is not sufficient**.

**Business needs:**
- Detect climate and sustainability trends as soon as they emerge  
- React quickly to changes in news sentiment and volume  
- Monitor global media coverage continuously  

Streaming enables **near real-time decision-making**, not delayed analysis.

---

## How It Works

The pipeline runs continuously:

1. News articles are fetched in real time from the GDELT Doc API  
2. Articles related to sustainability and climate topics are published to Kafka  
3. Spark processes the stream in real time  
4. Enriched results are stored in a database  
5. A dashboard displays live indicators and trends  

---

##  Architecture

- Producer polls GDELT Doc API (last 24h), filters sustainability keywords
- Producer publishes JSON events to Kafka topic: articles
- Spark Structured Streaming consumes Kafka, enriches (lang + sentiment)
- Spark writes enriched events to PostgreSQL
- Grafana queries Postgres to display live KPIs

##  Data
Each Kafka message is a JSON event (UTF-8). 

Example:
{
  "article_id": "https://...",
  "theme": "sustainability_energy_climate",
  "source": "example",
  "source_country": "US",
  "title": "Renewable energy investments surge...",
  "url": "https://...",
  "published_at": "2026-01-31T12:00:00Z",
  "provider": "gdelt_doc_api"
}


##  Processing Logic

1) Language detection

Uses langdetect on title + text
Output column: detected_lang

2) Sentiment (English-only)

Uses VADER (vaderSentiment)
Only computed if detected_lang == "en"

Output:

sentiment_score (compound score in [-1, 1])
sentiment label: positive / negative / neutral
Non-English events default to neutral to avoid misleading scoring.

3) Storage

Spark writes enriched events to Postgres table: articles_enriched
using foreachBatch + JDBC in append mode.

##  Visualization

<img width="1600" height="1200" alt="Streaming News Analytics-1769888701100" src="https://github.com/user-attachments/assets/97222bcb-e920-4af0-a111-150b6bd75062" />


##  Tech Stack

- Ingestion: GDELT Doc API + Python producer
- Streaming broker: Kafka (KRaft mode)
- Processing: Spark Structured Streaming (Kafka connector)
- Storage: PostgreSQL
- Visualization: Grafana
- Deployment: Docker Compose


## Roadmap (next improvements)

- Global trending topics detection from major international news outlets
- Climate & sustainability real-time trend detection (climate topics)
- English-only analytics layer for higher-quality sentiment and topic signals
- Add data quality checks + schema evolution

## Prerequisites

Before running the project, make sure you have the following installed:

- Docker (version 20+)
- Docker Compose (v2+)
- Git

#### Start the streaming pipeline
`docker compose up --build`

