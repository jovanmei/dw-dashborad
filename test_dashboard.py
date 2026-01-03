#!/usr/bin/env python3

# Simple test script to debug the broker_monitor.py issues

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Test basic imports
print("Testing basic imports...")
try:
    import pandas as pd
    import random
    from datetime import datetime, timedelta
    print("✓ Basic imports successful")
except Exception as e:
    print(f"✗ Basic imports failed: {e}")
    sys.exit(1)

# Test the load_simple_kafka_messages function
try:
    from src.dashboards.broker_monitor import load_simple_kafka_messages
    print("✓ Function import successful")
    
    # Test with demo mode
    print("Testing function with demo mode...")
    # Mock the session state for demo mode
    import streamlit as st
    if not hasattr(st, 'session_state'):
        st.session_state = {}
    st.session_state.demo_mode = True
    
    # Test loading orders
    orders_df = load_simple_kafka_messages('ecommerce_orders', 10)
    if orders_df is not None and len(orders_df) > 0:
        print(f"✓ Successfully generated {len(orders_df)} order records")
        print("First few records:")
        print(orders_df.head())
    else:
        print("✗ No orders generated")
        
    # Test loading customers
    customers_df = load_simple_kafka_messages('ecommerce_customers', 5)
    if customers_df is not None and len(customers_df) > 0:
        print(f"✓ Successfully generated {len(customers_df)} customer records")
        print("First few records:")
        print(customers_df.head())
    else:
        print("✗ No customers generated")
        
except Exception as e:
    print(f"✗ Function test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nAll tests completed!")
