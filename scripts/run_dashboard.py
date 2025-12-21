"""
Convenience script to run the Streamlit dashboard.

Usage:
    python scripts/run_dashboard.py

This will start the Streamlit server and open the dashboard in your browser.
Make sure to run 'python scripts/run_batch_etl.py' first to generate the data.
"""

import subprocess
import sys
import os

def main():
    print("🚀 Starting E-Commerce ETL Dashboard...")
    print("📊 Dashboard URL: http://localhost:8501")
    print("🔄 Run 'python scripts/run_batch_etl.py' first if you haven't generated data yet")
    print("=" * 60)
    
    try:
        # Get path to dashboard
        dashboard_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboards", "app_batch.py")
        
        # Run streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", dashboard_path,
            "--server.port", "8501",
            "--server.address", "localhost"
        ], check=True)
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running dashboard: {e}")
        print("💡 Make sure Streamlit is installed: pip install streamlit")

if __name__ == "__main__":
    main()