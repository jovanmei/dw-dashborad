# Streamlit Cloud Deployment Guide

## Fixed Issues ✅

The original error occurred because `run_simple_kafka_pipeline.py` uses signal handlers which aren't allowed in Streamlit Cloud's sandboxed environment.

## What I Fixed

1. **Created `streamlit_app.py`** - Streamlit Cloud entry point that runs the dashboard directly
2. **Updated dashboard** - Made it self-contained and Streamlit Cloud compatible
3. **Added sample data generation** - Dashboard can generate demo data without external processes
4. **Removed signal dependencies** - No more signal handlers that cause deployment errors

## Deployment Steps

### For Streamlit Cloud:

1. **Deploy using `streamlit_app.py`** as your main file (not `run_simple_kafka_pipeline.py`)

2. **Repository structure should be:**
   ```
   your-repo/
   ├── streamlit_app.py          # Main entry point for Streamlit Cloud
   ├── dashboards/
   │   └── app_simple_kafka.py   # Dashboard code
   ├── streaming/
   │   └── simple_kafka/
   │       └── server.py         # In-memory message broker
   ├── requirements.txt          # Dependencies (already correct)
   └── README.md
   ```

3. **In Streamlit Cloud settings:**
   - Main file path: `streamlit_app.py`
   - Python version: 3.9+ (recommended)

### How It Works Now

1. **Dashboard loads** - Automatically initializes Simple Kafka server
2. **Click "Initialize Simple Kafka Server"** - Creates topics and starts the in-memory broker
3. **Click "Generate Sample Data"** - Populates topics with demo e-commerce data
4. **View real-time dashboard** - All tabs show live data from the in-memory broker

### Features Available

- ✅ **Server Status** - Initialize and monitor the in-memory broker
- ✅ **Real-Time Metrics** - Live analytics from generated data
- ✅ **Orders** - Recent orders with filtering
- ✅ **Customers** - Customer events and segments  
- ✅ **Order Items** - Product categories and details
- ✅ **Fraud Alerts** - Suspicious pattern detection

### Local Development

For local development, you can still use:
```bash
# Option 1: Run complete pipeline (local only)
python run_simple_kafka_pipeline.py

# Option 2: Run dashboard directly (works locally and on Streamlit Cloud)
streamlit run streamlit_app.py
```

### Troubleshooting

**If you see "Simple Kafka server not available":**
1. Go to the "Server Status" tab
2. Click "Initialize Simple Kafka Server"
3. Click "Generate Sample Data"
4. Navigate to other tabs to see the data

**The dashboard is now fully self-contained and works in Streamlit Cloud's environment!**