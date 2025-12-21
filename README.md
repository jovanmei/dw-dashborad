# 🛒 E-Commerce Data Warehouse & Real-Time Dashboard

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://dw-dashboard.streamlit.app/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/Apache-Spark-orange.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion-green.svg)](#the-medallion-architecture)

A production-ready, modular data engineering project implementing a **Medallion Architecture** (Bronze/Silver/Gold) for an e-commerce data warehouse. This project demonstrates enterprise practices including batch ETL, real-time streaming, automated data quality analytics, and interactive BI dashboards.

---

## 📑 Table of Contents
1. [Business Context](#-business-context)
2. [Key Features](#-key-features)
3. [Architecture Deep Dive](#-architecture-deep-dive)
4. [File-by-File Technical Directory](#-file-by-file-technical-directory)
5. [Data Quality (DQ) Framework](#-data-quality-dq-framework)
6. [Getting Started](#-getting-started)
7. [Streaming Strategy](#-streaming-strategy)
8. [Testing](#-testing)

---

## 🎯 Business Context

**Problem**: Modern e-commerce platforms generate massive amounts of "dirty" data across siloed systems (Web logs, CRM, ERP). Businesses struggle to get a unified, reliable view of their performance in both historical and real-time contexts.

**Solution**: This platform provides an end-to-end data lakehouse solution that:
- **Cleanses** messy production data (missing values, duplicates, format inconsistencies).
- **Unifies** customer and order data into a dimensional model.
- **Predicts** customer value via RFM (Recency, Frequency, Monetary) segmentation.
- **Monitors** operational health through sub-second real-time dashboards.

---

## ✨ Key Features

- **🛡️ Robust ETL**: Medallion architecture ensures data lineage and reliability.
- **🔍 Auto-DQ (Data Quality)**: Integrated validation framework checking for nulls, duplicates, and referential integrity at every layer.
- **📈 Advanced Analytics**: 
    - **RFM Segmentation**: Automatically categorizes customers into "Champions", "At Risk", etc.
    - **Anomaly Detection**: Identifies revenue outliers using statistical thresholds.
    - **Fraud Monitoring**: Real-time detection of suspicious order patterns.
- **🚀 Dual-Mode Dashboards**:
    - **Batch BI**: Deep-dive historical analysis and portfolio-ready visualizations.
    - **Real-Time**: Low-latency monitoring via Spark Structured Streaming.
- **☁️ Cloud Optimized**: Fully compatible with Streamlit Cloud via an embedded "Simple Kafka" message broker.

---

## 🏗️ Architecture Deep Dive

### The Medallion Architecture
We use a three-layer approach to ensure data quality and scalability:

1.  **🥉 Bronze (Raw)**: 
    - **Source**: CSV/JSON/Parquet from multiple systems.
    - **Action**: Direct ingestion with minimal transformation. Preserves the "truth" of source data.
2.  **🥈 Silver (Enriched)**:
    - **Action**: Schema enforcement, data type conversion, name/email standardization, and joining across entities.
    - **Result**: A clean, queryable dimensional model (Fact & Dimension tables).
3.  **🥇 Gold (Curated)**:
    - **Action**: High-level aggregations and business logic (RFM, Monthly Revenue).
    - **Result**: Dashboard-ready datasets optimized for performance.

### Data Flow Diagram
```text
[Sources] -> [Bronze Layer] -> [DQ Checks] -> [Silver Layer] -> [Analytics] -> [Gold Layer] -> [Dashboards]
   ^              |               |               |              |              |              |
 (CSV/JSON)    (Parquet)      (Validation)     (Cleaned)      (RFM/Fraud)    (Aggregated)    (Streamlit)
```

---

## 📁 File-by-File Technical Directory

### ⚙️ Configuration (`/config`)
- `settings.py`: Centralized constants, file paths, and Spark session parameters. Uses dataclasses for clean, immutable config.
- `spark_config.py`: Specialized Spark tuning (shuffle partitions, memory management) optimized for local development.

### 📊 Dashboards (`/dashboards`)
- `app_batch.py`: The primary BI application. Visualizes historical data, RFM segments, and Data Quality metrics.
- `app_realtime.py`: High-concurrency dashboard designed for production Kafka streams.
- `app_simple_kafka.py`: Lightweight dashboard using the "Simple Kafka" mimic for Streamlit Cloud or local dev without Docker.

### 🚀 Execution Scripts (`/scripts`)
- `batch_etl_pipeline.py`: The "brain" of the ETL. Orchestrates the flow from Raw → Bronze → Silver → Gold.
- `run_batch_etl.py`: Simple entry point to trigger the full batch pipeline.
- `run_dashboard.py`: Convenience script to launch the Streamlit server with the correct environment variables.
- `init_raw_data.py`: Generates synthetic, "dirty" data (missing values, duplicates) to demonstrate the cleaning logic.
- `data_generation.py`: Library of generators for customers, orders, and products.
- `run_streaming.py`: Master launcher for all streaming modes (File-based, Simple Kafka, Production Kafka).
- `run_simple_kafka_pipeline.py`: A one-click script to start the entire Simple Kafka stack (Server + Generator + Dashboard).

### 🌊 Streaming Engine (`/streaming`)
- `/file_based`: Uses Spark's `readStream` to monitor local directories for new data drops.
- `/kafka`: Full enterprise setup with Docker-Compose (Kafka + Zookeeper) for sub-second latency.
- `/simple_kafka`: A Python-based in-memory broker that mimics Kafka behavior, allowing streaming features on restricted platforms like Streamlit Cloud.

### 🛠️ Utilities & Logic (`/utils`)
- `quality_checks.py`: The DQ engine. Contains logic for null counts, uniqueness checks, and referential integrity.
- `transformations.py`: Core Spark logic for data enrichment, building the fact table, and silver layer modeling.
- `analytics.py`: Encapsulates business logic like RFM scoring and statistical anomaly detection.
- `io.py`: Abstracted I/O handlers. Makes it easy to switch from local storage to S3/Azure Blob.
- `data_cleaning.py`: Specialized regex and rule-based cleaning for emails, dates, and names.

### 🧪 Quality Assurance (`/tests`)
- `test_environment.py`: Verifies Java, Python, and Spark dependencies are correctly installed.
- `test_dashboard.py`: Validates that the Gold layer data is compatible with Streamlit.
- `test_enhanced_kafka.py`: Stress-tests the embedded streaming pipeline.

---

## 🔍 Data Quality (DQ) Framework

Our `quality_checks.py` module runs at every stage. It computes:

| Pillar | Description | Action if Failed |
| :--- | :--- | :--- |
| **Completeness** | Null/NaN percentage per column | Logged; nulls filled or records dropped in Silver |
| **Uniqueness** | Duplicate primary key detection | Duplicates removed using Window functions |
| **Validity** | Date range and business rule validation | Records flagged as 'invalid' in monitoring |
| **Referential Integrity** | Ensures orders match existing customers | Orphaned orders moved to a 'quarantine' view |

---

## 🚀 Getting Started

### 1. Prerequisites
- **Python**: 3.8 or higher.
- **Java**: JDK 8 or 11 (required for PySpark).
- **Environment**: Windows, Linux, or macOS.

### 2. Installation
```bash
# Clone the repository
git clone <repository-url>
cd dw-dashboard

# Install dependencies
pip install -r requirements.txt
```

### 3. Execution Workflow (Standard Batch)
Follow these steps in order to see the full data lifecycle:

1.  **Generate Data**: `python scripts/init_raw_data.py` (Creates messy data in `data/raw`)
2.  **Process ETL**: `python scripts/run_batch_etl.py` (Moves data through Bronze → Silver → Gold)
3.  **View Dashboard**: `python scripts/run_dashboard.py` (Launches UI at `localhost:8501`)

---

## 🌊 Streaming Strategy

The project supports three streaming architectures:
1.  **File-Based**: Monitors a directory for new files (No Kafka required).
2.  **Simple Kafka**: Uses an in-memory Python broker (Best for Streamlit Cloud).
3.  **Production Kafka**: Full integration with Zookeeper and Kafka brokers (Requires Docker).

*See the [Real-Time Streaming Guide](docs/STREAMING_GUIDE.md) for advanced setup.*

---

## 📈 Business Use Cases
- **Marketing**: Target "High Value" segments identified by RFM.
- **Operations**: Monitor real-time order volume to identify bottlenecks.
- **Finance**: Track revenue anomalies to detect billing issues.
- **Governance**: Monitor DQ scores to ensure report reliability.

---

## ⚖️ License
Distributed under the MIT License. See `LICENSE` for more information.
