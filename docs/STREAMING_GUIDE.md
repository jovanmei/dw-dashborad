# 🌊 Real-Time Streaming & Kafka Guide

This guide covers everything you need to know about the real-time data engineering pipeline, from simple development setups to production-ready Kafka configurations.

---

## 📑 Table of Contents
1. [Architecture Overview](#-architecture-overview)
2. [Streaming Modes](#-streaming-modes)
3. [Simple Kafka Guide (Dev & Cloud)](#-simple-kafka-guide-dev--cloud)
4. [Production Kafka Setup](#-production-kafka-setup)
5. [Running the Pipeline](#-running-the-pipeline)
6. [Monitoring & Management](#-monitoring--management)
7. [Troubleshooting](#-troubleshooting)

---

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Data Generator │───▶│ Streaming Source │───▶│ Spark Streaming │
│  (Events)       │    │ (Files/Kafka)    │    │ Pipeline        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Real-Time       │◀───│ Memory Tables    │◀───│ Stream Processing│
│ Dashboard       │    │ (Live Data)      │    │ & Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │ Medallion Layers │◀───│ Batch Writes    │
                       │ (Bronze/Silver)  │    │ (Checkpointed)  │
                       └──────────────────┘    └─────────────────┘
```

### Data Flow
1. **Generator**: Creates continuous e-commerce events (orders, customers, items).
2. **Ingestion**: Spark Structured Streaming monitors a directory or Kafka topic.
3. **Processing**: Real-time cleaning, enrichment, validation, and fraud scoring.
4. **Storage**: Writes to Bronze (raw), Silver (enriched), and Memory (dashboard) layers.

---

## 🚀 Streaming Modes

The pipeline supports three distinct modes based on your infrastructure:

| Mode | Use Case | Setup Complexity | Dependencies |
| :--- | :--- | :--- | :--- |
| **File-Based** | Local testing / No Kafka | Low | None (uses local disk) |
| **Simple Kafka** | Dev / Streamlit Cloud | Low | In-memory broker (Python) |
| **Production Kafka** | Enterprise / Scalability | Medium | Zookeeper + Kafka Broker |

---

## 🛠️ Simple Kafka Guide (Dev & Cloud)

The "Simple Kafka" mode uses a lightweight, in-memory message broker. This is the **recommended mode for Streamlit Cloud** and quick local development.

### How to Use
1. **Run Complete Pipeline (Recommended)**:
   ```bash
   python scripts/run_simple_kafka_pipeline.py
   ```
   This starts the server, generator, and dashboard in one go.

2. **Manual Components**:
   - Start Server: `python streaming/simple_kafka/server_legacy.py`
   - Start Generator: `python streaming/simple_kafka/data_generator_legacy.py --burst`
   - Start Dashboard: `streamlit run dashboards/app_simple_kafka.py --server.port 8502`

### 🎯 What to Expect
After starting the data generator with `--burst`:
- **ecommerce_orders**: ~20+ messages (1 per event cycle)
- **ecommerce_customers**: ~4-6 messages (20% chance per cycle)
- **ecommerce_order_items**: ~40-100 messages (2-5 items per order)
- **ecommerce_fraud_alerts**: ~3-8 messages (15-40% chance based on order value)

---

## 🏭 Production Kafka Setup

For a full-scale deployment, follow these setup instructions.

### Prerequisites
- **Java 8+**: Required for native Kafka. Check with `java -version`.
- **Python Packages**: `pip install kafka-python pyspark streamlit plotly pandas`

### Option 1: Docker (Recommended)
1. **Start Services**:
   ```bash
   # Using our setup script
   python scripts/test_enhanced_kafka.py  # Or run docker-compose directly
   ```
2. **Access Points**:
   - Kafka Broker: `localhost:9092`
   - Kafka UI: `http://localhost:8080` (if running)

### Option 2: Windows Native Setup
1. **Download**: Kafka 2.13-3.6.0 from [Apache Kafka](https://kafka.apache.org/downloads).
2. **Start Zookeeper**: `bin\windows\zookeeper-server-start.bat config\zookeeper.properties`
3. **Start Kafka**: `bin\windows\kafka-server-start.bat config\server.properties`

---

## 🏃 Running the Pipeline

### Production Kafka Mode
```bash
# Start everything
python scripts/run_streaming.py --mode kafka --with-dashboard
```

### File-Based Mode
```bash
# Start everything
python scripts/run_streaming.py --mode file --with-dashboard
```

### Component Details
- **Data Generator**: `streaming/file_based/data_generator.py` or `streaming/kafka/setup.py`
- **Spark Pipeline**: `streaming/file_based/pipeline.py` or `streaming/kafka/run_pipeline.py`
- **Dashboard**: `dashboards/app_realtime.py`

---

## 📊 Monitoring & Management

### Testing & Verification
Before running full pipelines, verify your setup:
- **Environment**: `python tests/test_environment.py`
- **Simple Kafka**: `python tests/test_enhanced_kafka.py`
- **Production Kafka**: `python tests/test_kafka_production.py`

### Kafka Topic Monitoring
```bash
# Check message status
python utils/monitor_streaming.py --once
```

### Pipeline Performance
- **Bronze Layer**: `lake/bronze/orders_streaming/`
- **Silver Layer**: `lake/silver/orders_enriched_streaming/`
- **Checkpoints**: `checkpoints/streaming/`

---

## 🔧 Troubleshooting

| Issue | Solution |
| :--- | :--- |
| **"No brokers available"** | Check if Kafka/Docker is running. Use `docker ps`. |
| **"Simple Kafka not available"** | Ensure `scripts/run_simple_kafka_pipeline.py` is running. |
| **Memory Issues** | Increase Docker memory limit or Spark executor memory in `config/spark_config.py`. |
| **Import Errors** | Ensure you are running from the project root and have installed `requirements.txt`. |
| **Port 9092/8502 busy** | Use `netstat -ano | findstr :PORT` to find and kill the process. |
