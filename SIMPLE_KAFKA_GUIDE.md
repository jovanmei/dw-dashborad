# Simple Kafka Dashboard Guide

## Problem Solved ✅

The Streamlit dashboard was showing "Simple Kafka server not available" because it was looking for a `simple_kafka_server.py` file that didn't exist. 

## What I Fixed

1. **Created `simple_kafka_server.py`** - A standalone server launcher that the dashboard expects
2. **Created `run_simple_kafka_pipeline.py`** - A complete pipeline runner that starts everything
3. **Fixed dashboard configuration issues** - Resolved Streamlit page config conflicts

## How to Use

### Option 1: Run Complete Pipeline (Recommended)
```bash
python run_simple_kafka_pipeline.py
```
This starts:
- Simple Kafka server (in-memory message broker)
- Data generator (creates test e-commerce data)
- Streamlit dashboard at http://localhost:8502

### Option 2: Run Components Separately

1. **Start the server:**
```bash
python simple_kafka_server.py
```

2. **Generate test data:**
```bash
python streaming_data_generator_simple.py --burst --duration 30
```

3. **Run the dashboard:**
```bash
streamlit run dashboards/app_simple_kafka.py --server.port 8502
```

## What You'll See

The dashboard shows:
- **Server Status** - Monitor Simple Kafka broker and topics
- **Real-Time Metrics** - Live revenue, orders, and customer analytics  
- **Orders** - Recent orders with filtering capabilities
- **Customers** - Customer events and segments
- **Order Items** - Product categories and order details
- **Fraud Alerts** - Suspicious pattern detection

## Data Generated

The system creates realistic e-commerce data:
- **Orders**: 1 per cycle with realistic amounts and statuses
- **Customers**: 20% chance per cycle, creates new or updates existing
- **Order Items**: 1-5 items per order with product details
- **Fraud Alerts**: Generated for ~15% of orders (higher for high-value orders)

## Stopping the Pipeline

Press `Ctrl+C` to stop all components gracefully.

## Troubleshooting

If you see "Simple Kafka server not available":
1. Make sure `simple_kafka_server.py` exists (it should now)
2. Run the complete pipeline with `python run_simple_kafka_pipeline.py`
3. Wait a few seconds for data generation before viewing the dashboard

The system uses an in-memory message broker, so data is lost when you stop the server. This is perfect for development and testing!