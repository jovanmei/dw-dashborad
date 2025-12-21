#!/usr/bin/env python3
"""
Simple Kafka Pipeline Runner

This script starts the complete Simple Kafka pipeline:
1. Simple Kafka server (in-memory message broker)
2. Data generator (creates test data)
3. Streamlit dashboard (for visualization)
"""

import subprocess
import sys
import time
import signal
import os
from pathlib import Path


def start_simple_kafka_server():
    """Start the Simple Kafka server."""
    print("🚀 Starting Simple Kafka Server...")
    
    try:
        # Import and start the server
        sys.path.insert(0, str(Path(__file__).parent))
        from streaming.simple_kafka.server import start_simple_kafka_server
        server = start_simple_kafka_server()
        print("✅ Simple Kafka Server started successfully")
        return server
    except Exception as e:
        print(f"❌ Failed to start Simple Kafka server: {e}")
        return None


def start_data_generator():
    """Start the data generator."""
    print("🚀 Starting data generator...")
    
    cmd = [
        sys.executable, "streaming_data_generator_simple.py",
        "--interval", "2.0",
        "--burst"
    ]
    
    try:
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"✅ Data generator started (PID: {process.pid})")
        return process
    except Exception as e:
        print(f"❌ Failed to start data generator: {e}")
        return None


def start_dashboard(port=8502):
    """Start the Streamlit dashboard."""
    print(f"🚀 Starting dashboard on port {port}...")
    
    cmd = [
        sys.executable, "-m", "streamlit", "run", 
        "dashboards/app_simple_kafka.py",
        "--server.port", str(port),
        "--server.headless", "true"
    ]
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"✅ Dashboard started (PID: {process.pid})")
        print(f"   Access at: http://localhost:{port}")
        return process
    except Exception as e:
        print(f"❌ Failed to start dashboard: {e}")
        return None


def main():
    """Run the complete Simple Kafka pipeline."""
    print("🚀 Simple Kafka Pipeline Launcher")
    print("=" * 50)
    
    processes = []
    
    def cleanup():
        print("\n🛑 Stopping all processes...")
        for process in processes:
            if process and process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                except Exception:
                    pass
    
    def signal_handler(signum, frame):
        cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 1. Start Simple Kafka server
        server = start_simple_kafka_server()
        if not server:
            print("❌ Cannot continue without Simple Kafka server")
            return
        
        # 2. Start data generator
        generator_process = start_data_generator()
        if generator_process:
            processes.append(generator_process)
        
        # Wait a bit for data to be generated
        print("⏳ Waiting for initial data generation...")
        time.sleep(5)
        
        # 3. Start dashboard
        dashboard_process = start_dashboard()
        if dashboard_process:
            processes.append(dashboard_process)
        
        print("\n✅ Pipeline started successfully!")
        print("\n📊 Access Points:")
        print("   Dashboard: http://localhost:8502")
        print("\n💡 What's running:")
        print("   • Simple Kafka Server (in-memory message broker)")
        print("   • Data Generator (creating test e-commerce data)")
        print("   • Streamlit Dashboard (real-time visualization)")
        print("\n⏹️ Press Ctrl+C to stop all components")
        
        # Keep running and monitor processes
        while True:
            time.sleep(10)
            
            # Check if any process died
            for i, process in enumerate(processes):
                if process and process.poll() is not None:
                    print(f"⚠️ Process {i+1} stopped unexpectedly")
                    # Could restart here if needed
        
    except KeyboardInterrupt:
        print("\n🛑 Pipeline stopped by user")
    except Exception as e:
        print(f"\n❌ Pipeline error: {e}")
    finally:
        cleanup()


if __name__ == "__main__":
    main()