"""
Real-time Streamlit Dashboard for Simple Kafka Streaming Pipeline.

This dashboard reads directly from the Simple Kafka in-memory message broker
and displays live updates with auto-refresh capabilities.
"""

from __future__ import annotations

import time
import sys
import os
import requests
from typing import Optional, Dict, List

# Add project root to path if not already there
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Server configuration
SERVER_URL = "http://localhost:5051"


def safe_plotly_chart(fig, **kwargs):
    """Safely display plotly chart with compatibility for older Streamlit versions."""
    try:
        st.plotly_chart(fig, use_container_width=True, **kwargs)
    except TypeError:
        st.plotly_chart(fig, **kwargs)


def safe_dataframe(df, **kwargs):
    """Safely display dataframe with compatibility for older Streamlit versions."""
    try:
        st.dataframe(df, use_container_width=True, **kwargs)
    except TypeError:
        st.dataframe(df, **kwargs)


def generate_sample_data():
    """Generate sample data for demonstration when no data is available."""
    
    # In demo mode, we don't generate actual messages - we just refresh the dashboard
    if hasattr(st, 'session_state') and st.session_state.get('demo_mode', False):
        st.write("📊 Demo mode: Refreshing sample data...")
        # In demo mode, simply refresh the dashboard - the data generation happens in load_simple_kafka_messages
        if hasattr(st, 'rerun'):
            st.rerun()
        elif hasattr(st, 'experimental_rerun'):
            st.experimental_rerun()
        return True
    
    try:
        import json
        from datetime import datetime
        import random
        
        # Debug: Print connection attempt
        st.write("🔧 Attempting to connect to Simple Kafka server...")
        
        # Generate and send messages using REST API
        generated_count = 0
        
        for i in range(20):
            order_id = 1000 + i
            customer_id = 101 + (i % 10)
            total_amount = round(random.uniform(25.99, 1999.99), 2)
            
            # Occasionally generate high-value orders
            if random.random() < 0.1:
                total_amount = round(random.uniform(2000, 8000), 2)
            
            order = {
                'event_type': 'order',
                'event_timestamp': datetime.now().isoformat(),
                'order_id': order_id,
                'customer_id': customer_id,
                'order_date': datetime.now().strftime('%Y-%m-%d'),
                'status': random.choice(['pending', 'processing', 'completed', 'cancelled']),
                'total_amount': total_amount,
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal']),
                'source_system': 'demo_data'
            }
            
            # Send the order using REST API
            response = requests.post(f"{SERVER_URL}/produce/ecommerce_orders", json={'value': order})
            if response.status_code == 200:
                generated_count += 1
            
            # Generate customer event (30% chance)
            if random.random() < 0.3:
                customer = {
                    'event_type': 'customer_create',
                    'customer_id': customer_id,
                    'name': f'Customer {customer_id}',
                    'email': f'customer{customer_id}@example.com',
                    'customer_segment': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
                    'event_timestamp': datetime.now().isoformat()
                }
                response = requests.post(f"{SERVER_URL}/produce/ecommerce_customers", json={'value': customer})
                if response.status_code == 200:
                    generated_count += 1
            
            # Generate order items (1-3 per order)
            num_items = random.randint(1, 3)
            for j in range(num_items):
                item = {
                    'event_type': 'order_item',
                    'event_timestamp': datetime.now().isoformat(),
                    'item_id': (i * 10) + j,
                    'order_id': order_id,
                    'product_name': f'Product {random.randint(1, 50)}',
                    'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home']),
                    'quantity': random.randint(1, 3),
                    'unit_price': round(total_amount / (num_items * 2), 2),
                    'total_price': round(total_amount / num_items, 2)
                }
                response = requests.post(f"{SERVER_URL}/produce/ecommerce_order_items", json={'value': item})
                if response.status_code == 200:
                    generated_count += 1
            
            # Generate fraud alert (15% chance)
            if random.random() < 0.15 or total_amount > 2000:
                fraud_score = 3 if total_amount > 2000 else 2
                alert = {
                    'event_type': 'fraud_alert',
                    'event_timestamp': datetime.now().isoformat(),
                    'alert_id': f'FRAUD_{order_id}',
                    'order_id': order_id,
                    'customer_id': customer_id,
                    'fraud_score': fraud_score,
                    'risk_level': 'HIGH' if fraud_score >= 4 else 'MEDIUM',
                    'reasons': ['High transaction amount'] if total_amount > 2000 else ['Suspicious pattern'],
                    'total_amount': total_amount,
                    'requires_review': fraud_score >= 4
                }
                response = requests.post(f"{SERVER_URL}/produce/ecommerce_fraud_alerts", json={'value': alert})
                if response.status_code == 200:
                    generated_count += 1
        
        st.write(f"✅ Generated {generated_count} sample events!")
        
        # Refresh the dashboard to show new data
        if hasattr(st, 'rerun'):
            st.rerun()
        elif hasattr(st, 'experimental_rerun'):
            st.experimental_rerun()
        
        return True
        
    except Exception as e:
        st.error(f"Error generating sample data: {e}")
        # Show full error traceback for debugging
        import traceback
        st.code(traceback.format_exc())
        return False


def get_simple_kafka_status():
    """Get status from Simple Kafka server or return demo data."""
    
    # Return demo data if in demo mode
    if hasattr(st, 'session_state') and st.session_state.get('demo_mode', False):
        return {
            'server_running': True,
            'topics': {
                'ecommerce_orders': {
                    'partition_count': 3,
                    'total_messages': 156,
                    'partitions': {
                        0: {'messages': 52, 'latest_offset': 51},
                        1: {'messages': 52, 'latest_offset': 51},
                        2: {'messages': 52, 'latest_offset': 51}
                    }
                },
                'ecommerce_customers': {
                    'partition_count': 2,
                    'total_messages': 42,
                    'partitions': {
                        0: {'messages': 21, 'latest_offset': 20},
                        1: {'messages': 21, 'latest_offset': 20}
                    }
                },
                'ecommerce_order_items': {
                    'partition_count': 3,
                    'total_messages': 312,
                    'partitions': {
                        0: {'messages': 104, 'latest_offset': 103},
                        1: {'messages': 104, 'latest_offset': 103},
                        2: {'messages': 104, 'latest_offset': 103}
                    }
                },
                'ecommerce_fraud_alerts': {
                    'partition_count': 1,
                    'total_messages': 23,
                    'partitions': {
                        0: {'messages': 23, 'latest_offset': 22}
                    }
                }
            },
            'total_messages': 533
        }
    
    # Check server status using REST API directly for better reliability
    try:
        import requests
        # Try to connect to the REST server
        response = requests.get('http://localhost:5051/topics', timeout=2)
        if response.status_code == 200:
            topics = response.json()
            
            status = {
                'server_running': True,
                'topics': {},
                'total_messages': 0
            }
            
            # Get detailed info for each topic
            for topic in topics:
                topic_response = requests.get(f'http://localhost:5051/topics/{topic}', timeout=2)
                if topic_response.status_code == 200:
                    topic_info = topic_response.json()
                    status['topics'][topic] = topic_info
                    status['total_messages'] += topic_info.get('total_messages', 0)
            
            return status
        else:
            return {'error': f'HTTP {response.status_code}', 'server_running': False}
    
    except Exception as e:
        return {'error': str(e), 'server_running': False}


def load_simple_kafka_messages(topic: str, limit: int = 1000) -> Optional[pd.DataFrame]:
    """Load recent messages from Simple Kafka topic or generate demo data."""
    
    # Generate demo data if in demo mode
    if hasattr(st, 'session_state') and st.session_state.get('demo_mode', False):
        import random
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Generate synthetic data based on topic
        if topic == 'ecommerce_orders':
            # Generate sample orders
            orders = []
            now = datetime.now()
            
            for i in range(50):
                order_date = now - timedelta(minutes=random.randint(0, 120))
                total_amount = round(random.uniform(25.99, 1999.99), 2)
                
                # Occasionally generate high-value orders
                if random.random() < 0.1:
                    total_amount = round(random.uniform(2000, 8000), 2)
                
                order = {
                    'event_type': 'order',
                    'event_timestamp': order_date.isoformat(),
                    'order_id': 1000 + i,
                    'customer_id': 101 + (i % 10),
                    'order_date': order_date.strftime('%Y-%m-%d'),
                    'status': random.choice(['pending', 'processing', 'completed', 'cancelled']),
                    'total_amount': total_amount,
                    'payment_method': random.choice(['credit_card', 'debit_card', 'paypal']),
                    'source_system': 'demo_data',
                    '_kafka_timestamp': order_date,
                    '_kafka_offset': i,
                    '_kafka_partition': i % 3
                }
                orders.append(order)
            
            df = pd.DataFrame(orders)
            
        elif topic == 'ecommerce_customers':
            # Generate sample customers
            customers = []
            now = datetime.now()
            
            for i in range(10):
                customer = {
                    'event_type': 'customer_create',
                    'event_timestamp': (now - timedelta(days=i)).isoformat(),
                    'customer_id': 101 + i,
                    'name': f'Customer {101 + i}',
                    'email': f'customer{101 + i}@example.com',
                    'customer_segment': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
                    '_kafka_timestamp': now - timedelta(days=i),
                    '_kafka_offset': i,
                    '_kafka_partition': i % 2
                }
                customers.append(customer)
            
            df = pd.DataFrame(customers)
            
        elif topic == 'ecommerce_order_items':
            # Generate sample order items
            order_items = []
            now = datetime.now()
            
            for i in range(100):
                order_id = 1000 + (i // 2)  # 2 items per order on average
                order_items.append({
                    'event_type': 'order_item',
                    'event_timestamp': (now - timedelta(minutes=i)).isoformat(),
                    'item_id': 2000 + i,
                    'order_id': order_id,
                    'product_name': f'Product {random.randint(1, 50)}',
                    'category': random.choice(['Electronics', 'Clothing', 'Books', 'Home']),
                    'quantity': random.randint(1, 3),
                    'unit_price': round(random.uniform(10.99, 499.99), 2),
                    'total_price': round(random.uniform(10.99, 999.99), 2),
                    '_kafka_timestamp': now - timedelta(minutes=i),
                    '_kafka_offset': i,
                    '_kafka_partition': i % 3
                })
            
            df = pd.DataFrame(order_items)
            
        elif topic == 'ecommerce_fraud_alerts':
            # Generate sample fraud alerts
            fraud_alerts = []
            now = datetime.now()
            
            for i in range(15):
                order_id = 1000 + i
                total_amount = round(random.uniform(3000, 8000), 2)
                fraud_alerts.append({
                    'event_type': 'fraud_alert',
                    'event_timestamp': (now - timedelta(minutes=i*5)).isoformat(),
                    'alert_id': f'FRAUD_{order_id}',
                    'order_id': order_id,
                    'customer_id': 101 + (i % 10),
                    'fraud_score': random.randint(3, 5),
                    'risk_level': 'HIGH' if random.random() > 0.3 else 'MEDIUM',
                    'reasons': ['High transaction amount', 'Unusual purchasing pattern'],
                    'total_amount': total_amount,
                    'requires_review': random.random() > 0.5,
                    '_kafka_timestamp': now - timedelta(minutes=i*5),
                    '_kafka_offset': i,
                    '_kafka_partition': 0
                })
            
            df = pd.DataFrame(fraud_alerts)
        else:
            return None
        
        # Sort by timestamp (newest first)
        if '_kafka_timestamp' in df.columns:
            df = df.sort_values('_kafka_timestamp', ascending=False)
        elif 'event_timestamp' in df.columns:
            df = df.sort_values('event_timestamp', ascending=False)
        
        # Limit the results
        df = df.head(limit)
        
        # Convert timestamp columns
        if '_kafka_timestamp' in df.columns:
            df['_kafka_timestamp'] = pd.to_datetime(df['_kafka_timestamp'], errors='coerce')
        if 'event_timestamp' in df.columns:
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        return df
    
    # Live mode: Use REST API directly
    try:
        # Get topic info to determine partition count
        topic_info_response = requests.get(f"{SERVER_URL}/topics/{topic}")
        if topic_info_response.status_code != 200:
            return None
        
        topic_info = topic_info_response.json()
        partition_count = topic_info.get('partition_count', 1)
        
        # Get messages from all partitions
        all_messages = []
        
        for partition in range(partition_count):
            # Get messages for this partition
            messages_response = requests.get(
                f"{SERVER_URL}/consume/{topic}",
                params={'partition': partition, 'limit': limit}
            )
            if messages_response.status_code == 200:
                messages = messages_response.json()
                all_messages.extend(messages)
        
        if not all_messages:
            return None
        
        # Convert to DataFrame
        df_data = []
        for msg in all_messages[-limit:]:  # Get latest messages
            if 'value' in msg and msg['value']:
                row = msg['value'].copy()
                row['_kafka_timestamp'] = msg.get('_kafka_timestamp')
                row['_kafka_offset'] = msg.get('_kafka_offset')
                row['_kafka_partition'] = msg.get('_kafka_partition')
                df_data.append(row)
        
        if not df_data:
            return None
        
        df = pd.DataFrame(df_data)
        
        # Convert timestamp columns
        if '_kafka_timestamp' in df.columns:
            df['_kafka_timestamp'] = pd.to_datetime(df['_kafka_timestamp'], errors='coerce')
        if 'event_timestamp' in df.columns:
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        return df
        
    except Exception as e:
        st.error(f"Error loading messages from {topic}: {e}")
        return None


def calculate_simple_kafka_metrics(df: pd.DataFrame) -> Dict[str, any]:
    """Calculate metrics from Simple Kafka messages."""
    if df is None or len(df) == 0:
        return {}
    
    # Filter recent data (last hour)
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    
    # Use _kafka_timestamp if available, otherwise event_timestamp
    timestamp_col = '_kafka_timestamp' if '_kafka_timestamp' in df.columns else 'event_timestamp'
    if timestamp_col in df.columns:
        recent_df = df[df[timestamp_col] >= one_hour_ago]
    else:
        recent_df = df  # Use all data if no timestamp
    
    # Calculate metrics
    metrics = {
        'total_orders': len(df),
        'recent_orders': len(recent_df),
        'total_revenue': df['total_amount'].sum() if 'total_amount' in df.columns else 0,
        'recent_revenue': recent_df['total_amount'].sum() if 'total_amount' in recent_df.columns else 0,
        'avg_order_value': df['total_amount'].mean() if 'total_amount' in df.columns else 0,
        'unique_customers': df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
        'completed_orders': len(df[df['status'] == 'completed']) if 'status' in df.columns else 0,
        'completion_rate': len(df[df['status'] == 'completed']) / len(df) * 100 if 'status' in df.columns and len(df) > 0 else 0
    }
    
    return metrics


def detect_simple_kafka_fraud(df: pd.DataFrame) -> pd.DataFrame:
    """Detect potential fraud from Simple Kafka orders."""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    # Create fraud scoring
    fraud_df = df.copy()
    
    # Amount-based scoring
    fraud_df['amount_score'] = 0
    if 'total_amount' in fraud_df.columns:
        fraud_df.loc[fraud_df['total_amount'] > 5000, 'amount_score'] = 4
        fraud_df.loc[(fraud_df['total_amount'] > 3000) & (fraud_df['total_amount'] <= 5000), 'amount_score'] = 3
        fraud_df.loc[(fraud_df['total_amount'] > 2000) & (fraud_df['total_amount'] <= 3000), 'amount_score'] = 2
    
    # Status-based scoring
    fraud_df['status_score'] = 0
    if 'status' in fraud_df.columns:
        fraud_df.loc[fraud_df['status'].isin(['cancelled', 'refunded']), 'status_score'] = 1
    
    # Calculate total fraud score
    fraud_df['fraud_score'] = fraud_df['amount_score'] + fraud_df['status_score']
    
    # Filter potential fraud (score >= 3)
    fraud_alerts = fraud_df[fraud_df['fraud_score'] >= 3].copy()
    
    return fraud_alerts


def create_simple_kafka_status_section():
    """Display Simple Kafka server status."""
    st.header("🔄 Simple Kafka Status")
    
    status = get_simple_kafka_status()
    
    if not status.get('server_running', False):
        st.error("❌ Simple Kafka server not running")
        
        # Show error details if available
        if 'error' in status:
            st.info(f"Error: {status['error']}")
        
        # Try to initialize server - this will only work if the module is available locally
        if st.button("🚀 Initialize Simple Kafka Server"):
            try:
                # Only attempt this if we can import the server module
                from src.streaming.simple.server import start_simple_kafka_server
                start_simple_kafka_server()
                st.success("✅ Server initialized!")
                st.rerun()
            except ImportError:
                st.error("Cannot initialize server: Simple Kafka module not available")
            except Exception as e:
                st.error(f"Failed to initialize server: {e}")
        
        return
    
    # Server status
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.success("✅ Server Running")
        st.metric("Topics", len(status.get('topics', {})))
    
    with col2:
        st.success("✅ In-Memory Broker")
        st.metric("Total Messages", status.get('total_messages', 0))
    
    with col3:
        active_topics = sum(1 for topic_info in status.get('topics', {}).values() 
                           if topic_info.get('total_messages', 0) > 0)
        if active_topics > 0:
            st.success("✅ Active Topics")
        else:
            st.warning("⏳ No Messages")
        st.metric("Active Topics", active_topics)
    
    with col4:
        st.info("ℹ️ Development Mode")
        st.write("In-Memory Only")
    
    # Generate sample data if no messages
    if status.get('total_messages', 0) == 0:
        st.warning("⚠️ No data available")
        if st.button("🎲 Generate Sample Data"):
            with st.spinner("Generating sample data..."):
                if generate_sample_data():
                    st.success("✅ Sample data generated!")
                    st.rerun()
                else:
                    st.error("❌ Failed to generate sample data")
    
    # Topic details with enhanced display
    st.subheader("📋 Topic Details")
    
    topic_data = []
    topic_icons = {
        'ecommerce_orders': '🛒',
        'ecommerce_customers': '👥', 
        'ecommerce_order_items': '📦',
        'ecommerce_fraud_alerts': '🚨'
    }
    
    for topic_name, topic_info in status.get('topics', {}).items():
        if 'error' not in topic_info:
            icon = topic_icons.get(topic_name, '📁')
            message_count = topic_info.get('total_messages', 0)
            
            topic_data.append({
                'Topic': f"{icon} {topic_name}",
                'Partitions': topic_info.get('partition_count', 0),
                'Messages': message_count,
                'Status': '✅ Active' if message_count > 0 else '⚠️ Empty'
            })
    
    if topic_data:
        topic_df = pd.DataFrame(topic_data)
        safe_dataframe(topic_df)
        
        # Show quick action buttons
        st.subheader("🚀 Quick Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🎲 Generate More Data"):
                with st.spinner("Generating data..."):
                    if generate_sample_data():
                        st.success("✅ More data generated!")
                        st.rerun()
        
        with col2:
            if st.button("🔄 Refresh Status"):
                st.rerun()
        
        with col3:
            if st.button("📊 View All Topics"):
                st.info("All topic data is shown in the tabs above")
    else:
        st.info("No topics found. Click 'Initialize Simple Kafka Server' to create topics.")


def create_simple_kafka_metrics_section():
    """Display real-time metrics from Simple Kafka."""
    st.header("📊 Real-Time Metrics (Simple Kafka)")
    
    # Load data from ecommerce_orders topic
    orders_df = load_simple_kafka_messages('ecommerce_orders', 1000)
    
    if orders_df is not None and len(orders_df) > 0:
        st.success(f"📈 Live data from Simple Kafka ({len(orders_df):,} orders)")
        
        # Calculate metrics
        metrics = calculate_simple_kafka_metrics(orders_df)
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Orders", f"{metrics['total_orders']:,}")
        col2.metric("Total Revenue", f"${metrics['total_revenue']:,.2f}")
        col3.metric("Avg Order Value", f"${metrics['avg_order_value']:,.2f}")
        col4.metric("Completion Rate", f"{metrics['completion_rate']:.1f}%")
        
        # Recent activity (last hour)
        st.subheader("⚡ Recent Activity (Last Hour)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Recent Orders", f"{metrics['recent_orders']:,}")
        col2.metric("Recent Revenue", f"${metrics['recent_revenue']:,.2f}")
        col3.metric("Unique Customers", f"{metrics['unique_customers']:,}")
        
        # Time-based analysis
        if '_kafka_timestamp' in orders_df.columns or 'event_timestamp' in orders_df.columns:
            timestamp_col = '_kafka_timestamp' if '_kafka_timestamp' in orders_df.columns else 'event_timestamp'
            
            # Orders over time (last 2 hours, 5-minute buckets)
            st.subheader("📈 Orders Over Time")
            now = datetime.now()
            two_hours_ago = now - timedelta(hours=2)
            recent_data = orders_df[orders_df[timestamp_col] >= two_hours_ago].copy()
            
            if len(recent_data) > 0:
                # Create 5-minute buckets
                recent_data['time_bucket'] = recent_data[timestamp_col].dt.floor('5min')
                time_agg = recent_data.groupby('time_bucket').agg({
                    'order_id': 'count',
                    'total_amount': 'sum'
                }).reset_index()
                time_agg.columns = ['Time', 'Orders', 'Revenue']
                
                # Dual-axis chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Scatter(x=time_agg['Time'], y=time_agg['Orders'], 
                              name="Orders", line=dict(color='blue')),
                    secondary_y=False,
                )
                
                fig.add_trace(
                    go.Scatter(x=time_agg['Time'], y=time_agg['Revenue'], 
                              name="Revenue", line=dict(color='green')),
                    secondary_y=True,
                )
                
                fig.update_xaxes(title_text="Time")
                fig.update_yaxes(title_text="Order Count", secondary_y=False)
                fig.update_yaxes(title_text="Revenue ($)", secondary_y=True)
                fig.update_layout(title="Orders and Revenue Over Time (5-min buckets)")
                
                safe_plotly_chart(fig)
            else:
                st.info("No recent data in the last 2 hours.")
        
        # Status distribution
        if 'status' in orders_df.columns:
            st.subheader("📊 Order Status Distribution")
            status_counts = orders_df['status'].value_counts()
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(values=status_counts.values, names=status_counts.index,
                           title="Order Status Breakdown")
                safe_plotly_chart(fig)
            
            with col2:
                fig = px.bar(x=status_counts.index, y=status_counts.values,
                           title="Order Count by Status")
                fig.update_xaxes(title="Status")
                fig.update_yaxes(title="Count")
                safe_plotly_chart(fig)
    
    else:
        st.warning("⏳ No orders data available yet")
        st.info("""
        **To generate data:**
        1. Start the data generator: `python streaming_data_generator_simple.py`
        2. Or run the complete pipeline: `python run_simple_kafka_pipeline.py`
        3. Wait a few seconds for messages to be generated
        4. Refresh this dashboard
        """)


def create_simple_kafka_fraud_section():
    """Display fraud detection from Simple Kafka."""
    st.header("🚨 Fraud Detection (Simple Kafka)")
    
    # Load data
    orders_df = load_simple_kafka_messages('ecommerce_orders', 1000)
    
    if orders_df is not None and len(orders_df) > 0:
        # Detect fraud
        fraud_df = detect_simple_kafka_fraud(orders_df)
        
        if len(fraud_df) > 0:
            st.error(f"🚨 {len(fraud_df)} potential fraud alerts detected!")
            
            # Fraud metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Fraud Alerts", len(fraud_df))
            col2.metric("High Risk (Score ≥ 5)", len(fraud_df[fraud_df['fraud_score'] >= 5]))
            col3.metric("Fraud Amount", f"${fraud_df['total_amount'].sum():,.2f}")
            col4.metric("Avg Fraud Score", f"{fraud_df['fraud_score'].mean():.1f}")
            
            # Fraud score distribution
            st.subheader("📊 Fraud Score Distribution")
            fig = px.histogram(fraud_df, x='fraud_score', nbins=10,
                             title="Distribution of Fraud Scores")
            fig.update_xaxes(title="Fraud Score")
            fig.update_yaxes(title="Count")
            safe_plotly_chart(fig)
            
            # Top fraud cases
            st.subheader("🔍 Top Fraud Cases")
            fraud_display = fraud_df.sort_values('fraud_score', ascending=False).head(20)
            display_cols = ['order_id', 'customer_id', 'total_amount', 'fraud_score', 'status']
            if '_kafka_timestamp' in fraud_display.columns:
                display_cols.insert(2, '_kafka_timestamp')
            
            available_cols = [c for c in display_cols if c in fraud_display.columns]
            safe_dataframe(fraud_display[available_cols])
            
        else:
            st.success("✅ No fraud alerts detected")
            st.info("""
            **Fraud Detection Criteria:**
            - High-value orders (>$2,000): 2-4 points
            - Cancelled/refunded orders: +1 point
            - **Alert threshold**: Total score ≥ 3 points
            """)
    
    else:
        st.info("⏳ No data available for fraud analysis")


def create_simple_kafka_customers_section():
    """Display customer data from Simple Kafka."""
    st.header("👥 Customer Data (Simple Kafka)")
    
    # Load customer data
    customers_df = load_simple_kafka_messages('ecommerce_customers', 500)
    
    if customers_df is not None and len(customers_df) > 0:
        st.success(f"📊 Showing {len(customers_df)} customer events from Simple Kafka")
        
        # Customer metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", len(customers_df))
        
        unique_customers = customers_df['customer_id'].nunique() if 'customer_id' in customers_df.columns else 0
        col2.metric("Unique Customers", unique_customers)
        
        new_customers = len(customers_df[customers_df['event_type'] == 'customer_create']) if 'event_type' in customers_df.columns else 0
        col3.metric("New Customers", new_customers)
        
        updates = len(customers_df[customers_df['event_type'] == 'customer_update']) if 'event_type' in customers_df.columns else 0
        col4.metric("Customer Updates", updates)
        
        # Customer segments
        if 'customer_segment' in customers_df.columns:
            st.subheader("📊 Customer Segments")
            segment_counts = customers_df['customer_segment'].value_counts()
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(values=segment_counts.values, names=segment_counts.index,
                           title="Customer Segment Distribution")
                safe_plotly_chart(fig)
            
            with col2:
                fig = px.bar(x=segment_counts.index, y=segment_counts.values,
                           title="Customers by Segment")
                fig.update_xaxes(title="Segment")
                fig.update_yaxes(title="Count")
                safe_plotly_chart(fig)
        
        # Recent customers table
        st.subheader("📋 Recent Customer Events")
        display_cols = ['customer_id', 'name', 'email', 'customer_segment', 'event_type']
        if 'event_timestamp' in customers_df.columns:
            display_cols.append('event_timestamp')
        
        available_cols = [c for c in display_cols if c in customers_df.columns]
        safe_dataframe(customers_df[available_cols].head(50))
        
    else:
        st.info("⏳ No customer data available yet")
        st.info("Customer events are generated randomly (20% chance per order cycle)")


def create_simple_kafka_order_items_section():
    """Display order items data from Simple Kafka."""
    st.header("📦 Order Items (Simple Kafka)")
    
    # Load order items data
    items_df = load_simple_kafka_messages('ecommerce_order_items', 1000)
    
    if items_df is not None and len(items_df) > 0:
        st.success(f"📊 Showing {len(items_df)} order items from Simple Kafka")
        
        # Order items metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Items", len(items_df))
        
        unique_orders = items_df['order_id'].nunique() if 'order_id' in items_df.columns else 0
        col2.metric("Orders with Items", unique_orders)
        
        total_quantity = items_df['quantity'].sum() if 'quantity' in items_df.columns else 0
        col3.metric("Total Quantity", total_quantity)
        
        total_value = items_df['total_price'].sum() if 'total_price' in items_df.columns else 0
        col4.metric("Total Value", f"${total_value:,.2f}")
        
        # Category analysis
        if 'category' in items_df.columns:
            st.subheader("📊 Product Categories")
            category_stats = items_df.groupby('category').agg({
                'quantity': 'sum',
                'total_price': 'sum',
                'item_id': 'count'
            }).reset_index()
            category_stats.columns = ['Category', 'Total Quantity', 'Total Revenue', 'Item Count']
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(category_stats, x='Category', y='Total Revenue',
                           title="Revenue by Category")
                safe_plotly_chart(fig)
            
            with col2:
                fig = px.bar(category_stats, x='Category', y='Total Quantity',
                           title="Quantity by Category")
                safe_plotly_chart(fig)
            
            # Category table
            st.subheader("📋 Category Performance")
            safe_dataframe(category_stats)
        
        # Recent items table
        st.subheader("📋 Recent Order Items")
        display_cols = ['item_id', 'order_id', 'product_name', 'category', 'quantity', 'unit_price', 'total_price']
        if 'event_timestamp' in items_df.columns:
            display_cols.append('event_timestamp')
        
        available_cols = [c for c in display_cols if c in items_df.columns]
        safe_dataframe(items_df[available_cols].head(100))
        
    else:
        st.info("⏳ No order items data available yet")
        st.info("Order items are generated for every order (1-5 items per order)")


def create_simple_kafka_fraud_alerts_section():
    """Display fraud alerts from Simple Kafka."""
    st.header("🚨 Fraud Alerts (Simple Kafka)")
    
    # Load fraud alerts data
    fraud_df = load_simple_kafka_messages('ecommerce_fraud_alerts', 500)
    
    if fraud_df is not None and len(fraud_df) > 0:
        st.error(f"🚨 {len(fraud_df)} fraud alerts detected!")
        
        # Fraud metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Alerts", len(fraud_df))
        
        high_risk = len(fraud_df[fraud_df['risk_level'] == 'HIGH']) if 'risk_level' in fraud_df.columns else 0
        col2.metric("High Risk", high_risk)
        
        requires_review = len(fraud_df[fraud_df['requires_review'] == True]) if 'requires_review' in fraud_df.columns else 0
        col3.metric("Requires Review", requires_review)
        
        avg_score = fraud_df['fraud_score'].mean() if 'fraud_score' in fraud_df.columns else 0
        col4.metric("Avg Fraud Score", f"{avg_score:.1f}")
        
        # Risk level distribution
        if 'risk_level' in fraud_df.columns:
            st.subheader("📊 Risk Level Distribution")
            risk_counts = fraud_df['risk_level'].value_counts()
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(values=risk_counts.values, names=risk_counts.index,
                           title="Fraud Alerts by Risk Level",
                           color_discrete_map={'HIGH': 'red', 'MEDIUM': 'orange'})
                safe_plotly_chart(fig)
            
            with col2:
                if 'fraud_score' in fraud_df.columns:
                    fig = px.histogram(fraud_df, x='fraud_score', nbins=10,
                                     title="Fraud Score Distribution")
                    fig.update_xaxes(title="Fraud Score")
                    fig.update_yaxes(title="Count")
                    safe_plotly_chart(fig)
        
        # Fraud reasons analysis
        if 'reasons' in fraud_df.columns:
            st.subheader("🔍 Fraud Reasons Analysis")
            
            # Flatten reasons (they're stored as lists)
            all_reasons = []
            for reasons_list in fraud_df['reasons']:
                if isinstance(reasons_list, list):
                    all_reasons.extend(reasons_list)
                elif isinstance(reasons_list, str):
                    all_reasons.append(reasons_list)
            
            if all_reasons:
                reason_counts = pd.Series(all_reasons).value_counts()
                fig = px.bar(x=reason_counts.values, y=reason_counts.index,
                           orientation='h', title="Most Common Fraud Indicators")
                fig.update_xaxes(title="Frequency")
                fig.update_yaxes(title="Fraud Reason")
                safe_plotly_chart(fig)
        
        # High-priority alerts
        st.subheader("🚨 High-Priority Alerts")
        high_priority = fraud_df[fraud_df['requires_review'] == True] if 'requires_review' in fraud_df.columns else fraud_df
        
        if len(high_priority) > 0:
            display_cols = ['alert_id', 'order_id', 'customer_id', 'fraud_score', 'risk_level', 'total_amount']
            if 'event_timestamp' in high_priority.columns:
                display_cols.append('event_timestamp')
            
            available_cols = [c for c in display_cols if c in high_priority.columns]
            safe_dataframe(high_priority[available_cols])
        else:
            st.success("✅ No high-priority alerts requiring immediate review")
        
        # All fraud alerts table
        st.subheader("📋 All Fraud Alerts")
        display_cols = ['alert_id', 'order_id', 'fraud_score', 'risk_level', 'reasons', 'total_amount']
        if 'event_timestamp' in fraud_df.columns:
            display_cols.append('event_timestamp')
        
        available_cols = [c for c in display_cols if c in fraud_df.columns]
        safe_dataframe(fraud_df[available_cols])
        
    else:
        st.success("✅ No fraud alerts detected")
        st.info("""
        **Fraud Detection Criteria:**
        - High-value orders (>$2,000): 2-4 points
        - Cancelled/refunded orders: +1 point
        - Unusual behavior patterns: +2 points
        - Payment method risks: +3 points
        - **Alert threshold**: Total score ≥ 3 points
        
        Fraud alerts are generated for ~15% of orders (higher for high-value orders)
        """)


def create_simple_kafka_orders_section():
    """Display recent orders from Simple Kafka."""
    st.header("📦 Recent Orders (Simple Kafka)")
    
    # Load recent orders
    orders_df = load_simple_kafka_messages('ecommerce_orders', 200)
    
    if orders_df is not None and len(orders_df) > 0:
        st.success(f"📊 Showing latest {len(orders_df)} orders from Simple Kafka")
        
        # Filter controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'status' in orders_df.columns:
                status_filter = st.selectbox("Filter by Status", 
                                           ['All'] + list(orders_df['status'].unique()))
                if status_filter != 'All':
                    orders_df = orders_df[orders_df['status'] == status_filter]
        
        with col2:
            if 'total_amount' in orders_df.columns:
                min_amount = st.number_input("Min Order Amount", min_value=0.0, value=0.0)
                orders_df = orders_df[orders_df['total_amount'] >= min_amount]
        
        with col3:
            show_count = st.selectbox("Show Orders", [50, 100, 200], index=1)
            orders_df = orders_df.head(show_count)
        
        # Summary metrics
        if len(orders_df) > 0:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Filtered Orders", len(orders_df))
            col2.metric("Total Value", f"${orders_df['total_amount'].sum():,.2f}")
            col3.metric("Average Value", f"${orders_df['total_amount'].mean():,.2f}")
            col4.metric("Unique Customers", orders_df['customer_id'].nunique())
            
            # Orders table
            st.subheader("📋 Orders Table")
            display_cols = ['order_id', 'customer_id', 'order_date', 'status', 'total_amount']
            if '_kafka_timestamp' in orders_df.columns:
                display_cols.append('_kafka_timestamp')
            if '_kafka_offset' in orders_df.columns:
                display_cols.append('_kafka_offset')
            
            available_cols = [c for c in display_cols if c in orders_df.columns]
            safe_dataframe(orders_df[available_cols])
        else:
            st.info("No orders match the current filters.")
    
    else:
        st.info("⏳ No orders data available yet")


def main():
    """Main dashboard function."""
    # Only set page config if not already set
    try:
        st.set_page_config(
            page_title="Simple Kafka Real-Time Dashboard",
            layout="wide",
            initial_sidebar_state="expanded",
            page_icon="⚡"
        )
    except Exception:
        # Page config already set, continue
        pass
    
    # Initialize session state for demo mode
    if 'demo_mode' not in st.session_state:
        st.session_state.demo_mode = True  # Default to demo mode for online deployment
    
    st.title("⚡ Simple Kafka Real-Time Dashboard")
    st.markdown("""
    **Live Data Pipeline Dashboard** - Real-time analytics from Simple Kafka in-memory broker
    
    This dashboard provides:
    - **Server Status** - Monitor Simple Kafka broker and topics
    - **Real-Time Metrics** - Live revenue, orders, and customer analytics  
    - **Fraud Detection** - Suspicious pattern identification
    - **Order Stream** - Recent orders with filtering capabilities
    """)
    
    # Sidebar settings with demo mode toggle
    st.sidebar.header("⚙️ Dashboard Settings")
    st.session_state.demo_mode = st.sidebar.checkbox(
        "🟢 Run in Demo Mode", 
        value=st.session_state.demo_mode,
        help="Enable demo mode to use sample data without requiring a Simple Kafka server"
    )
    
    if st.session_state.demo_mode:
        st.sidebar.success("📊 Demo Mode: Using Sample Data")
        st.sidebar.info("In demo mode, the dashboard uses synthetic sample data instead of connecting to a Simple Kafka server.")
    else:
        st.sidebar.warning("🔴 Live Mode: Requires Simple Kafka Server")
        st.sidebar.info("In live mode, the dashboard connects to a running Simple Kafka server at http://localhost:5051.")
    
    # Auto-refresh configuration in sidebar
    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=False)  # Default off for Streamlit Cloud
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    
    # Show Simple Kafka status only if in live mode and server is available
    if st.session_state.demo_mode:
        st.sidebar.markdown("---")
        st.sidebar.success("📊 Demo Mode Active")
        st.sidebar.info("Using synthetic sample data for demonstration")
    else:
        # Simple Kafka status in sidebar
        st.sidebar.markdown("---")
        st.sidebar.subheader("📡 Simple Kafka Status")
        
        try:
            status = get_simple_kafka_status()
            if status.get('server_running', False):
                st.sidebar.success("✅ Server Running")
                st.sidebar.write(f"📁 Topics: {len(status.get('topics', {}))}")
                st.sidebar.write(f"📁 Messages: {status.get('total_messages', 0)}")
            else:
                st.sidebar.error("❌ Server Not Running")
        except Exception:
            st.sidebar.error("❌ Connection Failed")
        
        # Quick start in sidebar
        st.sidebar.markdown("---")
        st.sidebar.subheader("🚀 Quick Start")
        st.sidebar.info("Click 'Initialize Simple Kafka Server' in the Server Status tab to get started")
    
    # Check server status directly for live mode
    if not st.session_state.demo_mode:
        try:
            import requests
            response = requests.get('http://localhost:5051/topics', timeout=2)
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")
        except Exception as e:
            st.error("❌ Simple Kafka server not available")
            st.write("This dashboard is currently in live mode but cannot connect to a Simple Kafka server.")
            st.write("Options:")
            st.write("1. Switch to Demo Mode - Use the toggle in settings to enable sample data")
            st.write("2. For local development: Run `python scripts/run_streaming_simple.py`")
            st.write(f"Debug info: Server URL - http://localhost:5051, Error - {str(e)}")
            return
    
    
    # Navigation tabs
    st.markdown("---")
    
    # Use tabs for better organization
    if hasattr(st, "tabs"):
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🔄 Server Status", 
            "📊 Real-Time Metrics", 
            "🛒 Orders",
            "👥 Customers",
            "📦 Order Items", 
            "🚨 Fraud Alerts"
        ])
        
        with tab1:
            create_simple_kafka_status_section()
        
        with tab2:
            create_simple_kafka_metrics_section()
        
        with tab3:
            create_simple_kafka_orders_section()
        
        with tab4:
            create_simple_kafka_customers_section()
        
        with tab5:
            create_simple_kafka_order_items_section()
        
        with tab6:
            create_simple_kafka_fraud_alerts_section()
    
    else:
        # Fallback for older Streamlit versions
        page = st.selectbox(
            "Choose Dashboard Section:",
            ["🔄 Server Status", "📊 Real-Time Metrics", "🛒 Orders", "👥 Customers", "📦 Order Items", "🚨 Fraud Alerts"]
        )
        
        st.markdown("---")
        
        if page == "🔄 Server Status":
            create_simple_kafka_status_section()
        elif page == "📊 Real-Time Metrics":
            create_simple_kafka_metrics_section()
        elif page == "🛒 Orders":
            create_simple_kafka_orders_section()
        elif page == "👥 Customers":
            create_simple_kafka_customers_section()
        elif page == "📦 Order Items":
            create_simple_kafka_order_items_section()
        elif page == "🚨 Fraud Alerts":
            create_simple_kafka_fraud_alerts_section()
    
    # Auto-refresh logic (disabled by default for Streamlit Cloud)
    if auto_refresh:
        time.sleep(refresh_interval)
        try:
            if hasattr(st, "rerun"):
                st.rerun()
            elif hasattr(st, "experimental_rerun"):
                st.experimental_rerun()
        except Exception:
            # If auto-refresh fails, continue without it
            pass


if __name__ == "__main__":
    main()