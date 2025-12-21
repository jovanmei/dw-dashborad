#!/usr/bin/env python3
"""
Streamlit Cloud Compatible Simple Kafka Dashboard

This is the main entry point for Streamlit Cloud deployment.
It runs the Simple Kafka dashboard without requiring external processes.
"""

# Import the dashboard
from dashboards.app_simple_kafka import main

if __name__ == "__main__":
    main()