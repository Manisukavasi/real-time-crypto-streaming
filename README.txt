# Real-Time Cryptocurrency Streaming Pipeline

This project captures real-time BTC-USD price data using Coinbase WebSocket API, sends it to Kafka, and stores it in a PostgreSQL (Supabase) database.

## Features
- Real-time data streaming
- Kafka producer & consumer
- Supabase cloud PostgreSQL storage
- Graceful shutdown and reconnect logic

## Tech Stack
- Python
- websocket-client
- kafka-python
- psycopg2
- Docker, Kafka, Zookeeper
- Supabase PostgreSQL

## Project Structure
