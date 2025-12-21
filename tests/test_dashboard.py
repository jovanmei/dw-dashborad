"""
Simple test script to verify the real-time dashboard functions work correctly.
"""

import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.append('.')

def test_pipeline_status():
    """Test the pipeline status function."""
    print("🔍 Testing pipeline status function...")
    
    try:
        from app_realtime import get_pipeline_status
        
        status = get_pipeline_status()
        print(f"✅ Pipeline status retrieved successfully")
        print(f"   Generator running: {status['generator_running']}")
        print(f"   Pipeline running: {status['pipeline_running']}")
        print(f"   Input files: {status['input_files']}")
        print(f"   Bronze files: {status['bronze_files']}")
        print(f"   Silver files: {status['silver_files']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline status test failed: {e}")
        return False


def test_data_loading():
    """Test data loading functions."""
    print("\n🔍 Testing data loading functions...")
    
    try:
        from app_realtime import get_spark_session, load_bronze_orders, load_silver_orders
        
        # Test Spark session
        spark = get_spark_session()
        if spark:
            print("✅ Spark session created successfully")
            
            # Test Bronze loading
            bronze_df = load_bronze_orders(spark, 10)
            if bronze_df is not None:
                print(f"✅ Bronze data loaded: {len(bronze_df)} orders")
            else:
                print("ℹ️ No Bronze data available (expected if pipeline not running)")
            
            # Test Silver loading
            silver_df = load_silver_orders(spark, 10)
            if silver_df is not None:
                print(f"✅ Silver data loaded: {len(silver_df)} orders")
            else:
                print("ℹ️ No Silver data available (expected if pipeline not running)")
            
            spark.stop()
            return True
        else:
            print("❌ Could not create Spark session")
            return False
            
    except Exception as e:
        print(f"❌ Data loading test failed: {e}")
        return False


def test_fraud_detection():
    """Test fraud detection function."""
    print("\n🔍 Testing fraud detection function...")
    
    try:
        import pandas as pd
        from app_realtime import detect_fraud_from_data
        
        # Create test data
        test_data = pd.DataFrame({
            'order_id': [1, 2, 3, 4],
            'customer_id': [101, 102, 103, 104],
            'total_amount': [1000, 3500, 6000, 500],  # One high-value order
            'status': ['completed', 'completed', 'cancelled', 'completed']
        })
        
        fraud_df = detect_fraud_from_data(test_data)
        print(f"✅ Fraud detection completed")
        print(f"   Test orders: {len(test_data)}")
        print(f"   Fraud alerts: {len(fraud_df)}")
        
        if len(fraud_df) > 0:
            print(f"   Fraud scores: {fraud_df['fraud_score'].tolist()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Fraud detection test failed: {e}")
        return False


def test_metrics_calculation():
    """Test metrics calculation function."""
    print("\n🔍 Testing metrics calculation function...")
    
    try:
        import pandas as pd
        from datetime import datetime, timedelta
        from app_realtime import calculate_realtime_metrics
        
        # Create test data with timestamps
        now = datetime.now()
        test_data = pd.DataFrame({
            'order_id': [1, 2, 3, 4],
            'customer_id': [101, 102, 103, 101],
            'total_amount': [100, 200, 300, 150],
            'status': ['completed', 'completed', 'pending', 'completed'],
            'event_timestamp': [
                now - timedelta(minutes=30),
                now - timedelta(minutes=20),
                now - timedelta(minutes=10),
                now - timedelta(minutes=5)
            ]
        })
        
        metrics = calculate_realtime_metrics(test_data)
        print(f"✅ Metrics calculation completed")
        print(f"   Total orders: {metrics.get('total_orders', 0)}")
        print(f"   Total revenue: ${metrics.get('total_revenue', 0):.2f}")
        print(f"   Avg order value: ${metrics.get('avg_order_value', 0):.2f}")
        print(f"   Unique customers: {metrics.get('unique_customers', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Metrics calculation test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🧪 Testing Real-Time Dashboard Components")
    print("=" * 50)
    
    tests = [
        test_pipeline_status,
        test_data_loading,
        test_fraud_detection,
        test_metrics_calculation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Dashboard should work correctly.")
    else:
        print("⚠️ Some tests failed. Check the errors above.")
    
    print("\n💡 To test the full dashboard:")
    print("   streamlit run app_realtime.py --server.port 8502")


if __name__ == "__main__":
    main()