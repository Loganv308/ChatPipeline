# ChatPipeline
 
A silent Twitch chat scraper that extracts, sanitizes, and loads live chat messages into PostgreSQL via an ETL pipeline.
 
Like its namesake, ChatPipeline sits quietly in Twitch chat — watching everything, saying nothing, and recording it all.
 
## Overview
 
ChatPipeline connects to one or more Twitch channels via TwitchIO and listens to chat in real time. Each message passes through a sanitization pipeline before being batch-inserted into a PostgreSQL database every 2 seconds. It is designed to run continuously in the background, independently of any frontend or API layer.
 
Built as the data ingestion component of a larger Twitch analytics platform, ChatPipeline is intentionally minimal — its only job is to collect clean, reliable data.
 
## Features
 
- Real-time chat scraping across multiple Twitch channels simultaneously
- ETL pipeline — extracts, sanitizes, and loads each message before it hits the database
- Batch inserts every 2 seconds to minimize database load
- Duplicate-safe — `ON CONFLICT DO NOTHING` prevents double inserts on reconnect
- Automatic reconnection via TwitchIO
- Per-minute stats logging — messages received, inserted, skipped, and errors
 
## Tech Stack
 
- **Language:** Python 3.11+
- **Chat:** TwitchIO
- **Database:** PostgreSQL (via asyncpg)
- **Hosting:** Designed to run on a VPS or any always-on machine, utilizing Docker Containerization.
