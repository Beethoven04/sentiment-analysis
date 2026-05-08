# SentiStream — Real-Time Amazon Review Sentiment Analysis

> A production-grade Big Data pipeline that streams Amazon food reviews through Apache Kafka, classifies sentiment in real time using Apache Spark MLlib, stores predictions in MongoDB, and visualizes results on a live web dashboard.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Dataset](#dataset)
- [Machine Learning Pipeline](#machine-learning-pipeline)
- [Results](#results)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [How to Run](#how-to-run)
- [Dashboard](#dashboard)

---

## Overview

This project implements an end-to-end real-time sentiment analysis system on the Amazon Fine Food Reviews dataset (~568,000 reviews). The system:

1. **Trains** a multi-class sentiment classifier (positive / neutral / negative) using PySpark MLlib
2. **Streams** test reviews through Apache Kafka to simulate a real-time review feed
3. **Predicts** sentiment on each incoming review using the trained Spark model
4. **Stores** predictions in MongoDB with full metadata
5. **Displays** results on two dashboards: a live stream and a historical analytics view

---

## Architecture

```
Amazon Reviews (CSV)
        │
        ▼
┌───────────────┐     ┌─────────────────────────────┐
│  Kafka        │     │  Apache Kafka Cluster        │
│  Producer     │────▶│  Topic: amazon-reviews       │
│  (producer.py)│     │  3 Partitions · 1 Broker     │
└───────────────┘     └──────────────┬──────────────┘
                                     │
                                     ▼
                      ┌─────────────────────────────┐
                      │  Spark Streaming Consumer    │
                      │  (consumer.py)               │
                      │  • Reads micro-batches       │
                      │  • Loads saved ML model      │
                      │  • Predicts sentiment        │
                      └──────────────┬──────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │                                  │
                    ▼                                  ▼
        ┌───────────────────┐             ┌───────────────────┐
        │  MongoDB           │             │  Flask Web App     │
        │  amazon_sentiment  │◀────────────│  (app.py)         │
        │  .predictions      │             │  Online  Dashboard │
        └───────────────────┘             │  Offline Dashboard │
                                          └───────────────────┘
```

**Zookeeper** manages the Kafka cluster configuration and leader election between brokers.

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Stream Ingestion | Apache Kafka | 7.4.0 (Confluent) |
| Cluster Management | Apache Zookeeper | 7.4.0 (Confluent) |
| Stream Processing | Apache Spark Streaming | 3.5.1 |
| Machine Learning | PySpark MLlib | 3.5.1 |
| Storage | MongoDB | 6.0 |
| Containerization | Docker + Docker Compose | — |
| Web Framework | Flask | 3.x |
| Visualization | Chart.js | 4.4.0 |
| Language | Python | 3.11 |
| Notebooks | Jupyter | — |

---

## Project Structure

```
AmazonReview/
├── notebooks/
│   ├── 01_eda_preprocessing.ipynb   # EDA, lemmatization, feature engineering
│   └── 02_training.ipynb            # Model training, experiments, evaluation
├── data/
│   ├── train.csv                    # 80% of dataset (generated)
│   ├── val.csv                      # 10% validation set (generated)
│   └── test.csv                     # 10% test set — used for streaming
├── model/
│   ├── best_sentiment_model/        # Saved Spark PipelineModel
│   ├── label_mapping.json           # Maps numeric predictions to sentiment strings
│   └── model_info.json              # Best model metadata
├── streaming/
│   ├── producer.py                  # Kafka producer — streams test.csv row by row
│   └── consumer.py                  # Spark Streaming consumer — predicts + writes to MongoDB
├── app/
│   ├── app.py                       # Flask web application
│   └── templates/
│       ├── index.html               # Landing page
│       ├── online.html              # Live sentiment stream
│       └── offline.html             # Historical analytics dashboard
├── docker-compose.yml               # Zookeeper + Kafka + MongoDB
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## Dataset

**Source:** [Amazon Fine Food Reviews — Kaggle](https://www.kaggle.com/snap/amazon-fine-food-reviews)

| Field | Description |
|---|---|
| `Id` | Unique review identifier |
| `ProductId` | Unique product identifier |
| `UserId` | Unique user identifier |
| `HelpfulnessNumerator` | Number of users who found the review helpful |
| `HelpfulnessDenominator` | Total number of users who voted |
| `Score` | Product rating (1–5) |
| `Time` | Unix timestamp |
| `Summary` | Short review headline |
| `Text` | Full review text |

**Sentiment label (target variable):**
- `Score < 3` → **negative**
- `Score = 3` → **neutral**
- `Score > 3` → **positive**

**Class distribution:**

| Sentiment | Count | Percentage |
|---|---|---|
| Positive | ~446,000 | 78% |
| Negative | ~82,000 | 14% |
| Neutral | ~42,000 | 8% |

---

## Machine Learning Pipeline

### Preprocessing (`01_eda_preprocessing.ipynb`)

1. **Label creation** — numeric scores mapped to 3-class sentiment
2. **Helpfulness weighting** — `log(1 + helpful_votes)` used as sample weight
3. **Text cleaning** — HTML artifact removal, lowercasing, punctuation stripping
4. **Lemmatization** — POS-aware WordNetLemmatizer (not PorterStemmer)
5. **Negation preservation** — "not", "never", "no" explicitly kept from stopword removal
6. **Summary weighting** — review summary repeated 3× to amplify headline signal
7. **N-gram enrichment** — unigrams + bigrams + trigrams concatenated
8. **Stratified split** — 80% train / 10% val / 10% test

### Feature Pipeline

```
clean_text
    │
    ▼
Tokenizer → StopWordsRemover → HashingTF (80k features) → IDF (minDocFreq=3)
    │
    ▼
StringIndexer → Classifier
```

### Experiments (`02_training.ipynb`)

**Experiment 1 — N-gram configuration comparison** (5 configs tested)

| Config | Description |
|---|---|
| unigram | Individual words only |
| bigram | Word pairs only |
| trigram | Word triplets only |
| uni_bi | Words + pairs |
| uni_bi_tri | Words + pairs + triplets |

**Experiment 2 — RegParam grid search** (4 values tested: 0.001, 0.003, 0.005, 0.01)

**Experiment 3 — Final model comparison**

| Model | Training Strategy |
|---|---|
| Logistic Regression | Class weights (weightCol) |
| OneVsRest LinearSVC | Balanced undersampling |
| Naive Bayes | Balanced undersampling |

### Class Imbalance Handling

Two strategies applied:
- **Class weights** — `weight = total / (num_classes × class_count)` applied to LR
- **Balanced undersampling** — all classes capped at size of smallest class for SVC/NB

---

## Results

| Metric | Score |
|---|---|
| Weighted F1 (test) | **0.845** |
| Accuracy | 0.834 |
| Weighted Precision | 0.860 |
| Weighted Recall | 0.834 |
| Balanced Accuracy | 0.688 |

**Per-class recall:**

| Class | Recall |
|---|---|
| Positive | 89.0% |
| Negative | 73.8% |
| Neutral | 43.5% |

> Note: Neutral recall is inherently limited for TF-IDF approaches — neutral reviews linguistically overlap with both positive and negative classes. Academic benchmarks using BERT on this dataset report neutral F1 of ~55–60%.

---

## Prerequisites

- macOS / Linux
- Python 3.11
- Java 17 (`brew install openjdk@17`)
- Docker Desktop (running)
- Git

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/amazon-sentiment-analysis.git
cd amazon-sentiment-analysis
```

### 2. Download the dataset

Download `Reviews.csv` from [Kaggle](https://www.kaggle.com/snap/amazon-fine-food-reviews) and place it at:

```
FoodReviews/Reviews.csv
```

### 3. Create virtual environment and install dependencies

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Start Docker services

```bash
docker-compose up -d
docker-compose ps   # verify all 3 services are Up
```

### 5. Create Kafka topic

```bash
docker exec kafka kafka-topics --create \
  --topic amazon-reviews \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## How to Run

### Step 1 — Run the preprocessing notebook

Open Jupyter and run all cells in `notebooks/01_eda_preprocessing.ipynb`.

This generates `data/train.csv`, `data/val.csv`, `data/test.csv`.

Expected runtime: 20–30 minutes (POS tagging on 568k reviews).

### Step 2 — Train the model

Run all cells in `notebooks/02_training.ipynb`.

This runs n-gram experiments, regParam grid search, trains 3 classifiers,
and saves the best model to `model/best_sentiment_model/`.

Expected runtime: 1.5–2 hours for full experiment suite.

### Step 3 — Start the Spark Streaming consumer

Open Terminal 1:

```bash
source .venv/bin/activate
python3 streaming/consumer.py
```

Wait until you see:
```
Streaming started. Waiting for messages from Kafka...
```

### Step 4 — Start the Kafka producer

Open Terminal 2:

```bash
source .venv/bin/activate
python3 streaming/producer.py
```

The producer will stream one review per second from `data/test.csv`.
The consumer will predict sentiment and write results to MongoDB.

Verify MongoDB is receiving data (Terminal 3):

```bash
docker exec -it mongodb mongosh amazon_sentiment --eval "db.predictions.countDocuments();"
```

### Step 5 — Launch the web dashboard

Open Terminal 3:

```bash
source .venv/bin/activate
python3 app/app.py
```

Open your browser at **http://localhost:5000**

---

## Dashboard

### Online Mode (`/online`)
- Live feed of incoming reviews updated every 2 seconds
- Color-coded sentiment badges (green / red / yellow)
- Real-time accuracy tracker and distribution sidebar
- Star rating display and ground truth comparison

### Offline Mode (`/offline`)
- Total prediction KPIs
- Stacked bar chart — sentiment predictions by month
- Doughnut chart — overall sentiment distribution
- Product sentiment analyzer — enter any `ProductId` (e.g. `B001E4KFG0`) to see its pie chart breakdown

---

## Stopping the Project

```bash
# Stop producer and consumer with Ctrl+C in their terminals

# Stop Flask with Ctrl+C

# Stop Docker services
docker-compose down
```

To wipe MongoDB data and start fresh:

```bash
docker-compose down -v
```