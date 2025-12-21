"""
Test script to verify the project environment and dependencies.
"""

import os
import sys
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are available."""
    print("🔍 Checking Dependencies...")
    
    issues = []
    
    # Check PySpark
    try:
        import pyspark
        print(f"✅ PySpark {pyspark.__version__} - OK")
    except ImportError:
        issues.append("❌ PySpark not found. Install with: pip install pyspark")
    
    # Check Streamlit
    try:
        import streamlit
        print(f"✅ Streamlit {streamlit.__version__} - OK")
    except ImportError:
        issues.append("❌ Streamlit not found. Install with: pip install streamlit")
    
    # Check Plotly
    try:
        import plotly
        print(f"✅ Plotly {plotly.__version__} - OK")
    except ImportError:
        issues.append("❌ Plotly not found. Install with: pip install plotly")
    
    # Check Pandas
    try:
        import pandas
        print(f"✅ Pandas {pandas.__version__} - OK")
    except ImportError:
        issues.append("❌ Pandas not found. Install with: pip install pandas")
    
    # Check optional Kafka
    try:
        import kafka
        print(f"✅ kafka-python {kafka.__version__} - OK (optional)")
    except ImportError:
        print("ℹ️  kafka-python not found (optional - only needed for Kafka mode)")
    
    return issues

def check_directories():
    """Check if required directories exist or can be created."""
    print("\n📁 Checking Directories...")
    
    required_dirs = [
        "data/streaming/orders",
        "data/streaming/customers", 
        "checkpoints/streaming",
        "lake/bronze",
        "lake/silver",
        "lake/gold"
    ]
    
    for directory in required_dirs:
        try:
            Path(directory).mkdir(parents=True, exist_ok=True)
            print(f"✅ {directory} - OK")
        except Exception as e:
            print(f"❌ {directory} - Error: {e}")

def test_spark_session():
    """Test if Spark session can be created."""
    print("\n⚡ Testing Spark Session...")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("EnvironmentTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Test basic functionality
        test_data = [(1, "test"), (2, "data")]
        df = spark.createDataFrame(test_data, ["id", "value"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Spark session created and tested successfully (processed {count} rows)")
        return True
        
    except Exception as e:
        print(f"❌ Spark session test failed: {e}")
        return False

def main():
    print("🛠️  Project Environment Health Check")
    print("=" * 40)
    
    issues = check_dependencies()
    check_directories()
    spark_ok = test_spark_session()
    
    print("\n" + "=" * 40)
    if not issues and spark_ok:
        print("🎉 All core environment checks PASSED!")
    else:
        print("⚠️  Some issues were found:")
        for issue in issues:
            print(issue)
        if not spark_ok:
            print("❌ Spark session creation failed.")

if __name__ == "__main__":
    main()
