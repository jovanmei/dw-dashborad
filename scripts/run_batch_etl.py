#!/usr/bin/env python3
"""
Main launcher for batch ETL pipeline.
"""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    """Run the main batch ETL pipeline."""
    print("Starting Batch ETL Pipeline")
    print("=" * 50)
    
    # Import and run the main pipeline
    try:
        import batch_etl_pipeline
        batch_etl_pipeline.run_pipeline()
    except ImportError:
        print("Main pipeline not found. Please check file organization.")
    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()
