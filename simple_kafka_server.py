#!/usr/bin/env python3
"""
Simple Kafka Server Launcher

This script starts the Simple Kafka in-memory message broker and keeps it running.
The dashboard expects this file to exist and be runnable.
"""

import sys
import time
import signal
from pathlib import Path

# Add the streaming directory to the path so we can import the server
sys.path.insert(0, str(Path(__file__).parent))

try:
    from streaming.simple_kafka.server import start_simple_kafka_server, get_server
    print("✅ Simple Kafka server module loaded successfully")
except ImportError as e:
    print(f"❌ Failed to import Simple Kafka server: {e}")
    print("Make sure the streaming/simple_kafka/server.py file exists")
    sys.exit(1)


def main():
    """Start and run the Simple Kafka server."""
    print("🚀 Starting Simple Kafka Server")
    print("=" * 40)
    
    # Start the server
    server = start_simple_kafka_server()
    
    if not server:
        print("❌ Failed to start Simple Kafka server")
        sys.exit(1)
    
    print("\n✅ Simple Kafka Server is running!")
    print("   This is an in-memory message broker for development")
    print("   The server will keep running until you stop it with Ctrl+C")
    print("\n📊 To view the dashboard:")
    print("   streamlit run dashboards/app_simple_kafka.py")
    print("\n📈 To generate test data:")
    print("   python streaming_data_generator_simple.py --burst")
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}, shutting down...")
        print("✅ Simple Kafka Server stopped")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Keep the server running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Simple Kafka Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()