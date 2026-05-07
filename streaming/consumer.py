"""
consumer.py — Spark Streaming Consumer
Reads reviews from Kafka topic 'amazon-reviews', applies the saved
sentiment model, and writes predictions to MongoDB in real-time.
"""

import os
import json
import warnings
warnings.filterwarnings("ignore")

# Java 17 flags — must be set before any PySpark import
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PATH"]      = "/opt/homebrew/opt/openjdk@17/bin:" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)
from pyspark.ml import PipelineModel
from pymongo import MongoClient
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "amazon-reviews"
MODEL_PATH    = "/Users/beethoven/BigData/AmazonReview/model/best_sentiment_model"
LABEL_MAP     = "/Users/beethoven/BigData/AmazonReview/model/label_mapping.json"
MONGO_URI     = "mongodb://localhost:27017"
MONGO_DB      = "amazon_sentiment"
MONGO_COL     = "predictions"

# --- Load label mapping ---
with open(LABEL_MAP, "r") as f:
    label_mapping = json.load(f)
print(f"Label mapping loaded: {label_mapping}")

# --- Spark session with Kafka connector ---
_jvm_opts = " ".join([
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
])

spark = (
    SparkSession.builder
    .appName("AmazonSentimentStreaming")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.extraJavaOptions", _jvm_opts)
    .config("spark.executor.extraJavaOptions", _jvm_opts)
    # Kafka connector package — downloaded automatically on first run
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark version: {spark.version}")

# --- Load saved model ---
print(f"Loading model from: {MODEL_PATH}")
model = PipelineModel.load(MODEL_PATH)
print("Model loaded successfully.")

# --- Define schema for incoming Kafka JSON messages ---
# This must match exactly what the producer sends
schema = StructType([
    StructField("Id",         StringType(),  True),
    StructField("ProductId",  StringType(),  True),
    StructField("UserId",     StringType(),  True),
    StructField("Score",      IntegerType(), True),
    StructField("Summary",    StringType(),  True),
    StructField("Text",       StringType(),  True),
    StructField("clean_text", StringType(),  True),
    StructField("sentiment",  StringType(),  True),  # ground truth
    StructField("Time",       LongType(),    True),
])

# --- Read stream from Kafka ---
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    # Start from latest — only process new messages
    .option("startingOffsets", "earliest")    
    # Process up to 100 messages per micro-batch
    .option("maxOffsetsPerTrigger", 100)
    .load()
)

# --- Parse JSON messages ---
# Kafka delivers messages as binary — decode and parse JSON
parsed_stream = (
    raw_stream
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
    # Drop rows where clean_text is null or empty — model needs this column
    .filter(F.col("clean_text").isNotNull() & (F.col("clean_text") != ""))
)

# --- MongoDB writer function ---
# Called once per micro-batch with the batch DataFrame
def write_to_mongo(batch_df, batch_id):
    count = batch_df.count()
    print(f"Batch {batch_id}: received {count} rows")
    
    if count == 0:
        print(f"Batch {batch_id}: empty, skipping")
        return

    try:
        predictions = model.transform(batch_df)
        rows = predictions.select(
            "Id", "ProductId", "UserId", "Score",
            "Summary", "Text", "sentiment", "prediction", "Time"
        ).collect()

        print(f"Batch {batch_id}: got {len(rows)} predictions")

        client = MongoClient(MONGO_URI)
        db     = client[MONGO_DB]
        col    = db[MONGO_COL]

        # Test connection explicitly
        client.admin.command('ping')
        print(f"Batch {batch_id}: MongoDB connection OK")

        docs = []
        for row in rows:
            predicted_label = label_mapping.get(str(int(row["prediction"])), "unknown")
            review_date     = datetime.utcfromtimestamp(row["Time"]).strftime("%Y-%m-%d")
            docs.append({
                "reviewId"          : row["Id"],
                "productId"         : row["ProductId"],
                "userId"            : row["UserId"],
                "score"             : row["Score"],
                "summary"           : row["Summary"],
                "text"              : row["Text"],
                "groundTruth"       : row["sentiment"],
                "predictedSentiment": predicted_label,
                "predictionCorrect" : row["sentiment"] == predicted_label,
                "reviewDate"        : review_date,
                "processedAt"       : datetime.utcnow().isoformat(),
                "batchId"           : batch_id,
            })

        result = col.insert_many(docs)
        print(f"Batch {batch_id}: inserted {len(result.inserted_ids)} docs ✓")
        client.close()

    except Exception as e:
        print(f"Batch {batch_id}: ERROR — {e}")
        import traceback
        traceback.print_exc()

# --- Start streaming query ---
query = (
    parsed_stream.writeStream
    .foreachBatch(write_to_mongo)
    # Trigger every 5 seconds — balances latency and throughput
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation",
            "/Users/beethoven/BigData/AmazonReview/model/checkpoint")
    .start()
)

print("\nStreaming started. Waiting for messages from Kafka...")
print("Start the producer in another terminal to see predictions.\n")

# Keep streaming until manually stopped
query.awaitTermination()