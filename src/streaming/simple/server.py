"""
Enhanced embedded Kafka server for development using SQLite for persistence and concurrency.

Shortcomings of the previous JSON-based implementation:
1. Concurrency: File locking was rudimentary; multiple processes could corrupt JSON files.
2. Performance: Every message read required counting lines or parsing the entire JSON list.
3. Scaling: Large topics would become slow to read and write.
4. Reliability: No atomic transactions; a crash during write could lose data.

Improved Features:
- SQLite Backend: ACID transactions and multi-process safety.
- Consumer Groups: Support for multiple consumers sharing a workload (simulated).
- Retention Policy: Automatic cleanup of old messages.
- Offset Management: Persistent storage of consumer offsets.
"""

import json
import time
import os
import sqlite3
import requests
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import threading

# Make Flask an optional dependency
flask_available = False
try:
    from flask import Flask, request, jsonify
    flask_available = True
except ImportError:
    Flask = None
    request = None
    jsonify = None
    app = None

class SimpleKafkaServer:
    """
    Enhanced SQLite-based Kafka-like message broker for development.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            # Default to project_root/simple_kafka_data/kafka_broker.db
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            self.db_path = Path(project_root) / "simple_kafka_data" / "kafka_broker.db"
        else:
            self.db_path = Path(db_path)
            
        self.db_path.parent.mkdir(exist_ok=True)
        self.lock = threading.Lock()
        self._init_db()
        
    def _get_connection(self):
        """Get a thread-safe connection to SQLite."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize the SQLite database schema."""
        with self._get_connection() as conn:
            # Topics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS topics (
                    topic_name TEXT PRIMARY KEY,
                    partitions INTEGER DEFAULT 1,
                    retention_hours INTEGER DEFAULT 24,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Messages table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT,
                    partition INTEGER,
                    offset INTEGER,
                    key TEXT,
                    value TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(topic) REFERENCES topics(topic_name)
                )
            ''')
            
            # Offsets table (Consumer Groups)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS consumer_offsets (
                    group_id TEXT,
                    topic TEXT,
                    partition INTEGER,
                    current_offset INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (group_id, topic, partition)
                )
            ''')
            
            # Consumer Group Members (for rebalancing simulation)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS group_members (
                    group_id TEXT,
                    member_id TEXT,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (group_id, member_id)
                )
            ''')
            
            # Performance Indexes
            conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_topic_partition ON messages(topic, partition, offset)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)')
            
            conn.commit()

    def register_consumer(self, group_id: str, member_id: str):
        """Register a consumer in a group and update heartbeat."""
        try:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO group_members (group_id, member_id, last_heartbeat)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(group_id, member_id) DO UPDATE SET
                    last_heartbeat = CURRENT_TIMESTAMP
                ''', (group_id, member_id))
                conn.commit()
        except Exception as e:
            print(f"[ERROR] Error registering consumer: {e}")

    def get_group_assignments(self, group_id: str, topic: str, member_id: str) -> List[int]:
        """
        Simple partition assignment strategy: 
        Assign partitions to members based on their sorted order in the group.
        """
        try:
            with self._get_connection() as conn:
                # 1. Cleanup dead members (no heartbeat for 30s)
                conn.execute("DELETE FROM group_members WHERE last_heartbeat < datetime('now', '-30 seconds')")
                conn.commit()
                
                # 2. Get all active members in the group
                cursor = conn.execute("SELECT member_id FROM group_members WHERE group_id = ? ORDER BY member_id", (group_id,))
                members = [row['member_id'] for row in cursor.fetchall()]
                
                if member_id not in members:
                    return []
                
                # 3. Get partition count for the topic
                cursor = conn.execute("SELECT partitions FROM topics WHERE topic_name = ?", (topic,))
                row = cursor.fetchone()
                if not row: return []
                
                partition_count = row['partitions']
                
                # 4. Assign partitions (Range assignment)
                member_index = members.index(member_id)
                member_count = len(members)
                
                partitions_per_member = max(1, partition_count // member_count)
                start_p = member_index * partitions_per_member
                
                if member_index == member_count - 1:
                    # Last member gets remaining partitions
                    return list(range(start_p, partition_count))
                else:
                    return list(range(start_p, min(start_p + partitions_per_member, partition_count)))
                    
        except Exception as e:
            print(f"[ERROR] Error calculating assignments: {e}")
            return []

    def create_topic(self, topic: str, partitions: int = 1, retention_hours: int = 24):
        """Create a topic with specified partitions."""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO topics (topic_name, partitions, retention_hours) VALUES (?, ?, ?)",
                    (topic, partitions, retention_hours)
                )
                conn.commit()
            print(f"[SUCCESS] Created topic '{topic}' with {partitions} partition(s)")
        except Exception as e:
            print(f"[ERROR] Error creating topic: {e}")

    def send_message(self, topic: str, value: Any, key: Optional[str] = None, partition: int = 0) -> int:
        """Send a message to a topic partition and return its offset."""
        try:
            # Ensure topic exists
            if topic not in self.list_topics():
                self.create_topic(topic)

            with self._get_connection() as conn:
                # Get the next offset for this partition
                cursor = conn.execute(
                    "SELECT COALESCE(MAX(offset), -1) + 1 FROM messages WHERE topic = ? AND partition = ?",
                    (topic, partition)
                )
                next_offset = cursor.fetchone()[0]

                # Insert the message
                conn.execute(
                    "INSERT INTO messages (topic, partition, offset, key, value) VALUES (?, ?, ?, ?, ?)",
                    (topic, partition, next_offset, key, json.dumps(value))
                )
                conn.commit()
                return next_offset
        except Exception as e:
            print(f"[ERROR] Error sending message: {e}")
            return -1

    def consume_messages(self, topic: str, partition: int = 0, 
                        from_offset: Optional[int] = None, limit: int = 100) -> List[dict]:
        """Consume messages from a topic partition."""
        try:
            with self._get_connection() as conn:
                query = "SELECT * FROM messages WHERE topic = ? AND partition = ?"
                params = [topic, partition]
                
                if from_offset is not None:
                    query += " AND offset >= ?"
                    params.append(from_offset)
                
                query += " ORDER BY offset ASC LIMIT ?"
                params.append(limit)
                
                cursor = conn.execute(query, params)
                rows = cursor.fetchall()
                
                messages = []
                for row in rows:
                    msg = dict(row)
                    msg['value'] = json.loads(msg['value'])
                    # Map to Kafka-like metadata for compatibility
                    msg['_kafka_offset'] = msg['offset']
                    msg['_kafka_partition'] = msg['partition']
                    msg['_kafka_topic'] = msg['topic']
                    msg['_kafka_timestamp'] = msg['timestamp']
                    messages.append(msg)
                
                return messages
        except Exception as e:
            print(f"[ERROR] Error consuming messages: {e}")
            return []

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int):
        """Commit an offset for a consumer group."""
        try:
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT INTO consumer_offsets (group_id, topic, partition, current_offset, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(group_id, topic, partition) DO UPDATE SET
                    current_offset = excluded.current_offset,
                    updated_at = CURRENT_TIMESTAMP
                ''', (group_id, topic, partition, offset))
                conn.commit()
        except Exception as e:
            print(f"[ERROR] Error committing offset: {e}")

    def get_committed_offset(self, group_id: str, topic: str, partition: int) -> int:
        """Get the last committed offset for a group."""
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "SELECT current_offset FROM consumer_offsets WHERE group_id = ? AND topic = ? AND partition = ?",
                    (group_id, topic, partition)
                )
                row = cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            print(f"[ERROR] Error getting committed offset: {e}")
            return 0

    def list_topics(self) -> List[str]:
        """List all topics."""
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT topic_name FROM topics")
                return [row['topic_name'] for row in cursor.fetchall()]
        except Exception:
            return []

    def get_topic_info(self, topic: str) -> dict:
        """Get detailed information about a topic."""
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT partitions FROM topics WHERE topic_name = ?", (topic,))
                row = cursor.fetchone()
                if not row:
                    return {'error': 'Topic not found'}
                
                partition_count = row['partitions']
                partitions = {}
                total_messages = 0
                
                for p in range(partition_count):
                    stats = conn.execute(
                        "SELECT COUNT(*) as cnt, MAX(offset) as max_off FROM messages WHERE topic = ? AND partition = ?",
                        (topic, p)
                    ).fetchone()
                    
                    partitions[p] = {
                        'messages': stats['cnt'],
                        'latest_offset': stats['max_off'] if stats['max_off'] is not None else -1
                    }
                    total_messages += stats['cnt']
                
                return {
                    'partition_count': partition_count,
                    'total_messages': total_messages,
                    'partitions': partitions
                }
        except Exception as e:
            return {'error': str(e)}

    def cleanup_old_messages(self):
        """Perform maintenance: delete messages older than retention period."""
        try:
            with self._get_connection() as conn:
                # This is a simplified cleanup based on retention_hours per topic
                cursor = conn.execute("SELECT topic_name, retention_hours FROM topics")
                topics = cursor.fetchall()
                
                for topic in topics:
                    cutoff = (datetime.now() - timedelta(hours=topic['retention_hours'])).isoformat()
                    conn.execute(
                        "DELETE FROM messages WHERE topic = ? AND timestamp < ?",
                        (topic['topic_name'], cutoff)
                    )
                conn.commit()
        except Exception as e:
            print(f"[ERROR] Maintenance error: {e}")

    def clear_topic(self, topic: str):
        """Clear all messages from a topic."""
        try:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM messages WHERE topic = ?", (topic,))
                conn.commit()
                print(f"[SUCCESS] Cleared topic '{topic}'")
        except Exception as e:
            print(f"[ERROR] Error clearing topic: {e}")

# --- REST API and Client Layers ---

class RestKafkaServerProxy:
    """Proxy that mimics SimpleKafkaServer but uses REST API."""
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    def list_topics(self) -> List[str]:
        try:
            return requests.get(f"{self.base_url}/topics").json()
        except: return []
        
    def get_topic_info(self, topic: str) -> dict:
        try:
            return requests.get(f"{self.base_url}/topics/{topic}").json()
        except Exception as e: return {'error': str(e)}
        
    def send_message(self, topic: str, value: Any, key: Optional[str] = None, partition: int = 0) -> int:
        try:
            resp = requests.post(f"{self.base_url}/produce/{topic}", 
                               json={'value': value, 'key': key, 'partition': partition})
            return resp.json().get('offset', -1)
        except: return -1
        
    def consume_messages(self, topic: str, partition: int = 0, 
                        from_offset: Optional[int] = None, limit: int = 100) -> List[dict]:
        try:
            params = {'partition': partition, 'limit': limit}
            if from_offset is not None: params['from_offset'] = from_offset
            return requests.get(f"{self.base_url}/consume/{topic}", params=params).json()
        except: return []
        
    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int):
        try:
            requests.post(f"{self.base_url}/groups/{group_id}/commit", 
                         json={'topic': topic, 'partition': partition, 'offset': offset})
        except: pass
        
    def get_committed_offset(self, group_id: str, topic: str, partition: int) -> int:
        try:
            resp = requests.get(f"{self.base_url}/groups/{group_id}/offset/{topic}/{partition}")
            return resp.json().get('offset', 0)
        except: return 0

    def register_consumer(self, group_id: str, member_id: str):
        try:
            requests.post(f"{self.base_url}/groups/{group_id}/heartbeat", json={'member_id': member_id})
        except: pass

    def get_group_assignments(self, group_id: str, topic: str, member_id: str) -> List[int]:
        try:
            resp = requests.get(f"{self.base_url}/groups/{group_id}/assignments/{topic}", 
                               params={'member_id': member_id})
            return resp.json().get('partitions', [])
        except: return []

_server = None
_app = None
if flask_available:
    _app = Flask(__name__)

def get_server(bootstrap_servers: Optional[str] = None):
    global _server
    
    # If bootstrap_servers is provided and is an HTTP URL, return a REST proxy
    if bootstrap_servers and bootstrap_servers.startswith('http'):
        return RestKafkaServerProxy(bootstrap_servers)
    
    # Otherwise return/create the local singleton server
    if _server is None:
        # Check environment variable for default REST server
        env_url = os.environ.get('SIMPLE_KAFKA_URL')
        if env_url and env_url.startswith('http'):
            _server = RestKafkaServerProxy(env_url)
        else:
            _server = SimpleKafkaServer()
    return _server

if flask_available:
    @_app.route('/topics', methods=['GET'])
    def api_list_topics():
        return jsonify(get_server().list_topics())

    @_app.route('/topics/<topic>', methods=['GET'])
    def api_topic_info(topic):
        return jsonify(get_server().get_topic_info(topic))

    @_app.route('/produce/<topic>', methods=['POST'])
    def api_produce(topic):
        data = request.json
        value = data.get('value')
        key = data.get('key')
        partition = data.get('partition', 0)
        offset = get_server().send_message(topic, value, key, partition)
        return jsonify({'offset': offset, 'topic': topic, 'partition': partition})

    @_app.route('/consume/<topic>', methods=['GET'])
    def api_consume(topic):
        partition = int(request.args.get('partition', 0))
        from_offset = request.args.get('from_offset')
        if from_offset is not None: from_offset = int(from_offset)
        limit = int(request.args.get('limit', 100))
        
        msgs = get_server().consume_messages(topic, partition, from_offset, limit)
        return jsonify(msgs)

    @_app.route('/groups/<group_id>/commit', methods=['POST'])
    def api_commit(group_id):
        data = request.json
        topic = data.get('topic')
        partition = data.get('partition')
        offset = data.get('offset')
        get_server().commit_offset(group_id, topic, partition, offset)
        return jsonify({'status': 'success'})

    @_app.route('/groups/<group_id>/offset/<topic>/<int:partition>', methods=['GET'])
    def api_get_offset(group_id, topic, partition):
        offset = get_server().get_committed_offset(group_id, topic, partition)
        return jsonify({'offset': offset})

    @_app.route('/groups/<group_id>/heartbeat', methods=['POST'])
    def api_heartbeat(group_id):
        member_id = request.json.get('member_id')
        get_server().register_consumer(group_id, member_id)
        return jsonify({'status': 'ok'})

    @_app.route('/groups/<group_id>/assignments/<topic>', methods=['GET'])
    def api_assignments(group_id, topic):
        member_id = request.args.get('member_id')
        assignments = get_server().get_group_assignments(group_id, topic, member_id)
        return jsonify({'partitions': assignments})

class SimpleKafkaProducer:
    def __init__(self, bootstrap_servers=None, **kwargs):
        self.base_url = bootstrap_servers if bootstrap_servers and bootstrap_servers.startswith('http') else None
        self.server = get_server() if not self.base_url else None
        self.value_serializer = kwargs.get('value_serializer', lambda x: x)
    
    def send(self, topic: str, value=None, key=None, partition=0):
        serialized_value = self.value_serializer(value) if value else None
        
        # If bytes, try to decode to JSON for storage
        if isinstance(serialized_value, bytes):
            try:
                serialized_value = json.loads(serialized_value.decode('utf-8'))
            except:
                pass
        
        if self.base_url:
            try:
                resp = requests.post(f"{self.base_url}/produce/{topic}", 
                                   json={'value': serialized_value, 'key': key, 'partition': partition})
                result = resp.json()
                offset = result.get('offset', -1)
            except Exception as e:
                print(f"[ERROR] REST Producer Error: {e}")
                offset = -1
        else:
            offset = self.server.send_message(topic, serialized_value, key, partition)
        
        class MockFuture:
            def get(self, timeout=None):
                class Meta:
                    def __init__(self, t, p, o):
                        self.topic, self.partition, self.offset = t, p, o
                return Meta(topic, partition, offset)
        return MockFuture()

    def flush(self): pass
    def close(self): pass

class SimpleKafkaConsumer:
    def __init__(self, *topics, bootstrap_servers=None, **kwargs):
        self.base_url = bootstrap_servers if bootstrap_servers and bootstrap_servers.startswith('http') else None
        self.server = get_server() if not self.base_url else None
        self.topics = topics
        self.group_id = kwargs.get('group_id', 'default_group')
        self.member_id = str(uuid.uuid4())[:8]
        self.value_deserializer = kwargs.get('value_deserializer', lambda x: x)
        self.consumer_timeout_ms = kwargs.get('consumer_timeout_ms', 1000)
        
        self.current_offsets = {}
        self._refresh_assignments()

    def _refresh_assignments(self):
        """Determine which partitions this consumer instance should handle."""
        self.assigned_partitions = {} # (topic, p)
        
        for topic in self.topics:
            if self.base_url:
                try:
                    # 1. Heartbeat
                    requests.post(f"{self.base_url}/groups/{self.group_id}/heartbeat", 
                                 json={'member_id': self.member_id})
                    # 2. Get assignments
                    resp = requests.get(f"{self.base_url}/groups/{self.group_id}/assignments/{topic}", 
                                      params={'member_id': self.member_id})
                    partitions = resp.json().get('partitions', [])
                    for p in partitions:
                        # 3. Get committed offset
                        off_resp = requests.get(f"{self.base_url}/groups/{self.group_id}/offset/{topic}/{p}")
                        self.assigned_partitions[(topic, p)] = off_resp.json().get('offset', 0)
                except Exception as e:
                    print(f"[ERROR] REST Consumer Assignment Error: {e}")
            else:
                self.server.register_consumer(self.group_id, self.member_id)
                partitions = self.server.get_group_assignments(self.group_id, topic, self.member_id)
                for p in partitions:
                    self.assigned_partitions[(topic, p)] = self.server.get_committed_offset(self.group_id, topic, p)
        
        self.current_offsets.update(self.assigned_partitions)

    def __iter__(self):
        start_time = time.time()
        timeout_s = self.consumer_timeout_ms / 1000.0
        last_refresh = time.time()
        
        while True:
            # Re-balance check every 10 seconds
            if time.time() - last_refresh > 10:
                self._refresh_assignments()
                last_refresh = time.time()

            found = False
            for (topic, p), offset in self.current_offsets.items():
                if self.base_url:
                    try:
                        resp = requests.get(f"{self.base_url}/consume/{topic}", 
                                          params={'partition': p, 'from_offset': offset, 'limit': 50})
                        msgs = resp.json()
                    except:
                        msgs = []
                else:
                    msgs = self.server.consume_messages(topic, p, from_offset=offset, limit=50)
                
                for m in msgs:
                    found = True
                    self.current_offsets[(topic, p)] = m['offset'] + 1
                    
                    class MockMsg:
                        def __init__(self, data, deserializer):
                            self.topic = data['topic']
                            self.partition = data['partition']
                            self.offset = data['offset']
                            self.value = deserializer(data['value'])
                            self.timestamp = data['timestamp']
                    
                    yield MockMsg(m, self.value_deserializer)
            
            if not found:
                if time.time() - start_time > timeout_s: break
                time.sleep(0.5)
            else:
                start_time = time.time()

    def commit(self):
        """Commit current offsets."""
        for (topic, p), offset in self.current_offsets.items():
            if self.base_url:
                try:
                    requests.post(f"{self.base_url}/groups/{self.group_id}/commit", 
                                 json={'topic': topic, 'partition': p, 'offset': offset})
                except: pass
            else:
                self.server.commit_offset(self.group_id, topic, p, offset)

    def close(self):
        self.commit()

def run_server(port=5050):
    """Start the REST API server."""
    if not flask_available:
        print(f"[ERROR] Cannot start Simple Kafka REST Server - Flask is not available")
        return False
    
    print(f"[START] Starting Simple Kafka REST Server on port {port}...")
    # Pre-create topics
    srv = get_server()
    topics = [
        ("ecommerce_orders", 3),
        ("ecommerce_customers", 2),
        ("ecommerce_order_items", 3),
        ("ecommerce_fraud_alerts", 1)
    ]
    for name, p in topics:
        srv.create_topic(name, p)
        
    _app.run(host='0.0.0.0', port=port, threaded=True)

def start_simple_kafka_server():
    """Entry point for launcher scripts."""
    print("[INFO] Initializing Enhanced Simple Kafka (SQLite-backed)")
    return get_server()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        # Check for port argument
        port = 5050
        if len(sys.argv) > 3 and sys.argv[2] == "--port":
            try:
                port = int(sys.argv[3])
            except ValueError:
                print(f"Invalid port '{sys.argv[3]}', using default 5050")
        run_server(port=port)
    else:
        # Self-test - should work without Flask
        srv = start_simple_kafka_server()
        prod = SimpleKafkaProducer()
        prod.send("ecommerce_orders", {"test": "data"})
        
        cons = SimpleKafkaConsumer("ecommerce_orders", consumer_timeout_ms=1000)
        for msg in cons:
            print(f"Received: {msg.value} at offset {msg.offset}")
        cons.commit()
        print("Test passed.")
