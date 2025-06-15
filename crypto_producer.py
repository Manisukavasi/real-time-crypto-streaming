import json
import time
import signal
import sys
from kafka import KafkaProducer
from websocket import WebSocketApp

# Global variable to track last sent time
last_sent_time = 0

# Coinbase WebSocket endpoint
SOCKET = "wss://ws-feed.exchange.coinbase.com"

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def on_message(ws, message):
    global last_sent_time
    data = json.loads(message)
    if data.get("type") == "ticker":
        now = time.time()
        # Send only if 30 seconds have passed since last send
        if now - last_sent_time >= 30:
            price_data = {
                "symbol": data.get("product_id"),
                "price": data.get("price"),
                "time": data.get("time")
            }
            print(f"Sending to Kafka: {price_data}")
            producer.send("crypto_prices", price_data)
            last_sent_time = now

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened")
    subscribe_message = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
    }
    ws.send(json.dumps(subscribe_message))

# Graceful exit handler
exit_flag = False

def signal_handler(sig, frame):
    global exit_flag
    print("\n[Exit] Stopping WebSocket producer...")
    exit_flag = True

signal.signal(signal.SIGINT, signal_handler)

# Run WebSocket connection with reconnect logic
while not exit_flag:
    try:
        ws = WebSocketApp(
            SOCKET,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
    except Exception as e:
        print(f"Error: {e}")
    if not exit_flag:
        print("Reconnecting in 5 seconds...")
        time.sleep(5)

print("Exited cleanly.")
sys.exit(0)
