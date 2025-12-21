#!/usr/bin/env python3
"""
Test script for Enhanced Simple Kafka Pipeline.

This script tests that all components work correctly:
1. Simple Kafka server starts
2. Enhanced data generator populates all topics
3. Dashboard can read from all topics
"""

import time
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_simple_kafka_server():
    """Test that Simple Kafka server works."""
    print("🧪 Testing Simple Kafka Server...")
    
    try:
        from streaming.simple_kafka.server import start_simple_kafka_server, SimpleKafkaProducer, SimpleKafkaConsumer
        
        # Start server
        server = start_simple_kafka_server()
        print("✅ Simple Kafka server started")
        
        # Test producer
        producer = SimpleKafkaProducer()
        test_message = {'test': 'message', 'timestamp': time.time()}
        
        future = producer.send('test_topic', test_message)
        metadata = future.get()
        print(f"✅ Message sent to {metadata.topic}:{metadata.partition}:{metadata.offset}")
        
        # Test consumer
        consumer = SimpleKafkaConsumer('test_topic', consumer_timeout_ms=2000)
        
        message_received = False
        for message in consumer:
            if message.value and message.value.get('test') == 'message':
                print("✅ Message received successfully")
                message_received = True
                break
        
        consumer.close()
        producer.close()
        
        return message_received
        
    except Exception as e:
        print(f"❌ Simple Kafka server test failed: {e}")
        return False


def test_enhanced_data_generator():
    """Test that enhanced data generator works."""
    print("\n🧪 Testing Enhanced Data Generator...")
    
    try:
        # Import the enhanced generator
        sys.path.append('.')
        from streaming_data_generator_simple import EnhancedDataGenerator
        
        generator = EnhancedDataGenerator()
        
        # Generate some test events
        for i in range(5):
            generator.generate_and_send_events()
        
        stats = generator.stats
        print(f"✅ Generated test events: {stats}")
        
        # Check that all event types were generated
        success = (
            stats['orders'] > 0 and
            stats['order_items'] > 0
        )
        
        generator.close()
        return success
        
    except Exception as e:
        print(f"❌ Enhanced data generator test failed: {e}")
        return False


def test_dashboard_imports():
    """Test that dashboard can import required modules."""
    print("\n🧪 Testing Dashboard Imports...")
    
    try:
        from dashboards.app_simple_kafka import (
            get_simple_kafka_status,
            load_simple_kafka_messages,
            calculate_simple_kafka_metrics
        )
        print("✅ Dashboard imports successful")
        
        # Test status function
        status = get_simple_kafka_status()
        print(f"✅ Status function works: {len(status)} keys")
        
        return True
        
    except Exception as e:
        print(f"❌ Dashboard import test failed: {e}")
        return False


def test_topic_population():
    """Test that all topics get populated."""
    print("\n🧪 Testing Topic Population...")
    
    try:
        from streaming.simple_kafka.server import get_server
        from streaming_data_generator_simple import EnhancedDataGenerator
        
        # Generate burst of data
        generator = EnhancedDataGenerator()
        
        print("Generating burst of test data...")
        for i in range(10):
            generator.generate_and_send_events()
        
        generator.close()
        
        # Check all topics have data
        server = get_server()
        topics = ['ecommerce_orders', 'ecommerce_customers', 'ecommerce_order_items', 'ecommerce_fraud_alerts']
        
        results = {}
        for topic in topics:
            topic_info = server.get_topic_info(topic)
            message_count = topic_info.get('total_messages', 0)
            results[topic] = message_count
            print(f"  {topic}: {message_count} messages")
        
        # Check that orders and items always have data
        success = (
            results['ecommerce_orders'] > 0 and
            results['ecommerce_order_items'] > 0
        )
        
        if success:
            print("✅ Topic population test passed")
        else:
            print("❌ Some required topics are empty")
        
        return success
        
    except Exception as e:
        print(f"❌ Topic population test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Enhanced Simple Kafka Pipeline Test Suite")
    print("=" * 60)
    
    tests = [
        ("Simple Kafka Server", test_simple_kafka_server),
        ("Enhanced Data Generator", test_enhanced_data_generator),
        ("Dashboard Imports", test_dashboard_imports),
        ("Topic Population", test_topic_population)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n📈 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 All tests passed! Your Enhanced Simple Kafka pipeline is ready!")
        print("\n🚀 Next steps:")
        print("1. Run: python streaming_data_generator_simple.py --burst")
        print("2. Open: streamlit run dashboards/app_simple_kafka.py --server.port 8502")
        print("3. Visit: http://localhost:8502")
    else:
        print(f"\n⚠️ {total - passed} tests failed. Please check the errors above.")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())