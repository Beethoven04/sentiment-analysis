"""
producer.py — Kafka Producer
Reads test.csv row by row and publishes each review as a JSON message
to the 'amazon-reviews' Kafka topic, simulating a real-time stream.
"""

import json
import time
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

# --- Configuration ---
BASE_DIR     = Path(__file__).resolve().parent.parent
KAFKA_BROKER = "localhost:9092"
TOPIC        = "amazon-reviews"
TEST_CSV     = BASE_DIR / "data" / "test.csv"
DELAY        = 1.0  # seconds between messages — simulates real-time flow

# --- Initialize producer ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    # Serialize Python dict → JSON bytes for Kafka
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # Wait for broker acknowledgment before continuing
    acks="all",
    # Retry up to 3 times on transient failures
    retries=3
)

print(f"Connected to Kafka broker: {KAFKA_BROKER}")
print(f"Publishing to topic      : {TOPIC}")
print(f"Reading from             : {TEST_CSV}")
print(f"Delay between messages   : {DELAY}s")
print("-" * 50)

# --- Load test data ---
# We only stream the test set — these are the 10% reserved for real-time demo
df = pd.read_csv(TEST_CSV)
df = df.dropna(subset=["Text", "Score", "ProductId"])

total = len(df)
print(f"Total reviews to stream  : {total:,}")
print("Starting stream... Press Ctrl+C to stop.\n")

# --- Stream reviews one by one ---
for i, row in df.iterrows():
    message = {
        "Id"        : str(row.get("Id", "")),
        "ProductId" : str(row.get("ProductId", "")),
        "UserId"    : str(row.get("UserId", "")),
        "Score"     : int(row.get("Score", 0)),
        "Summary"   : str(row.get("Summary", "")),
        "Text"      : str(row.get("Text", "")),
        "clean_text": str(row.get("clean_text", "")),
        "sentiment" : str(row.get("sentiment", "")),  # ground truth label
        "Time"      : int(row.get("Time", 0))
    }

    # Publish to Kafka — key is ProductId so same product goes to same partition
    producer.send(
        TOPIC,
        key=message["ProductId"].encode("utf-8"),
        value=message
    )

    count = i + 1
    print(f"[{count:>6}/{total}] Sent review {message['Id']} | "
          f"Product: {message['ProductId']} | "
          f"Sentiment: {message['sentiment']}")

    # Flush every 100 messages to avoid buffer buildup
    if count % 100 == 0:
        producer.flush()

    time.sleep(DELAY)

# Final flush to ensure all messages are delivered
producer.flush()
producer.close()
print("\nAll reviews streamed successfully.")