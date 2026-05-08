"""
app.py — Flask Web Dashboard
Two modes:
- Online  (/online)  : real-time stream of predictions, polls MongoDB every 2 seconds
- Offline (/offline) : historical charts from all stored predictions
"""

from flask import Flask, jsonify, render_template
from pymongo import MongoClient
from collections import defaultdict

app = Flask(__name__)

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB  = "amazon_sentiment"
MONGO_COL = "predictions"

def get_col():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][MONGO_COL], client

# ─── Routes ───────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/online")
def online():
    return render_template("online.html")

@app.route("/offline")
def offline():
    return render_template("offline.html")

# ─── API: latest predictions for online dashboard ─────────
@app.route("/api/latest")
def api_latest():
    col, client = get_col()
    docs = list(
        col.find({}, {"_id": 0})
        .sort("processedAt", -1)
        .limit(20)
    )
    client.close()
    return jsonify(docs)

# ─── API: predictions per date for offline bar chart ──────
@app.route("/api/by_date")
def api_by_date():
    col, client = get_col()
    docs = list(col.find({}, {"_id": 0, "reviewDate": 1, "predictedSentiment": 1}))
    client.close()

    counts = defaultdict(lambda: {"positive": 0, "negative": 0, "neutral": 0})
    for doc in docs:
        date = doc.get("reviewDate", "unknown")
        # Group by year-month instead of exact date
        month     = date[:7] if len(date) >= 7 else date
        sentiment = doc.get("predictedSentiment", "unknown")
        if sentiment in counts[month]:
            counts[month][sentiment] += 1

    result = sorted([
        {"date": d, **v} for d, v in counts.items()
    ], key=lambda x: x["date"])

    return jsonify(result)

# ─── API: sentiment breakdown for specific product ────────
@app.route("/api/product/<product_id>")
def api_product(product_id):
    col, client = get_col()
    docs = list(col.find(
        {"productId": product_id},
        {"_id": 0, "predictedSentiment": 1}
    ))
    client.close()

    counts = {"positive": 0, "negative": 0, "neutral": 0}
    for doc in docs:
        s = doc.get("predictedSentiment", "unknown")
        if s in counts:
            counts[s] += 1

    return jsonify({"productId": product_id, "counts": counts, "total": len(docs)})

if __name__ == "__main__":
    app.run(debug=True, port=5000)