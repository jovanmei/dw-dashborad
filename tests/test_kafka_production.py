"""
Production Kafka connectivity test for the streaming pipeline.
Requires a running Kafka broker (e.g., via docker-compose).
"""

import json
import time
import uuid
from datetime import datetime

def test_kafka_basic():
    """Test basic Kafka producer and consumer functionality."""
    print("🧪 Testing Production Kafka Connectivity")
    print("=" * 50)
    
    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import KafkaError, NoBrokersAvailable
        
        bootstrap_servers = ['localhost:9092']
        test_topic = 'test_topic_prod'
        
        print("📤 Testing Kafka Producer...")
        
        # Test Producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000
        )
        
        # Send test message
        test_message = {
            'id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'message': 'Hello Production Kafka!',
            'test': True
        }
        
        try:
            future = producer.send(test_topic, test_message)
            record_metadata = future.get(timeout=10)
            print(f"✅ Message sent successfully!")
            print(f"   Topic: {record_metadata.topic}")
            print(f"   Partition: {record_metadata.partition}")
            print(f"   Offset: {record_metadata.offset}")
        except KafkaError as e:
            print(f"❌ Failed to send message: {e}")
            return False
        finally:
            producer.close()
        
        print("\n📥 Testing Kafka Consumer...")
        
        # Test Consumer
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000,
            auto_offset_reset='earliest'
        )
        
        message_received = False
        start_time = time.time()
        
        for message in consumer:
            if message.value and message.value.get('id') == test_message['id']:
                print(f"✅ Test message received!")
                print(f"   Content: {message.value}")
                message_received = True
                break
            
            # Timeout after 10 seconds
            if time.time() - start_time > 10:
                break
        
        consumer.close()
        
        if message_received:
            print("\n🎉 Production Kafka connectivity test PASSED!")
            return True
        else:
            print("\n❌ Test message not received within timeout.")
            return False
            
    except ImportError:
        print("❌ kafka-python not installed")
        return False
    except NoBrokersAvailable:
        print("❌ No Kafka brokers available at localhost:9092")
        return False
    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        return False

if __name__ == "__main__":
    test_kafka_basic()
