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
import random
import threading
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

# Global server instance
kafka_server = None
server_thread = None

# Flask availability flag
flask_available = False
try:
    # Try to import Flask to check availability
    import flask
    flask_available = True
    # Import Simple Kafka Server components if Flask is available
    from src.streaming.simple.server import SimpleKafkaServer, start_simple_kafka_server, run_server
    st.write("✅ Flask available for Simple Kafka Server")
except ImportError:
    st.write("⚠️ Flask not available - Simple Kafka Server functionality limited")
    SimpleKafkaServer = None
    start_simple_kafka_server = None
    run_server = None

# Function to start Kafka server in background thread
def start_kafka_server_in_background():
    """Start Simple Kafka server in a background thread if Flask is available."""
    global kafka_server, server_thread
    
    if not flask_available:
        st.error("❌ Cannot start Simple Kafka Server: Flask dependency not available")
        st.info("This feature requires Flask to be installed. Demo mode will continue to work.")
        return False
    
    if kafka_server is None:
        st.write("🚀 Initializing Simple Kafka Server...")
        
        try:
            # Initialize server instance
            kafka_server = start_simple_kafka_server()
            
            # Pre-create topics
            topics = [
                ("ecommerce_orders", 3),
                ("ecommerce_customers", 2),
                ("ecommerce_order_items", 3),
                ("ecommerce_fraud_alerts", 1)
            ]
            for name, partitions in topics:
                kafka_server.create_topic(name, partitions)
            
            st.write("✅ Simple Kafka Server initialized successfully!")
            
            # Start REST server in background thread
            def run_server_thread():
                try:
                    run_server(port=5051)
                except Exception as e:
                    st.error(f"Server error: {e}")
            
            server_thread = threading.Thread(target=run_server_thread, daemon=True)
            server_thread.start()
            st.write("🌐 REST API server started on port 5051")
            
            return True
        except Exception as e:
            st.error(f"Failed to start Kafka server: {e}")
            import traceback
            st.code(traceback.format_exc())
            return False
    return False


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
    
    try:
        import json
        from datetime import datetime
        import random
        
        # Demo mode handling - generate realistic data and refresh
        if hasattr(st, 'session_state') and st.session_state.get('demo_mode', False):
            st.write("📊 Demo mode: Generating new sample data...")
            
            # Increment data generation counter to ensure new data each time
            if 'data_generation_count' not in st.session_state:
                st.session_state.data_generation_count = 0
            st.session_state.data_generation_count += 1
            
            st.write(f"   Generation #{st.session_state.data_generation_count}")
            
            # Refresh the dashboard to show new data
            if hasattr(st, 'rerun'):
                st.rerun()
            elif hasattr(st, 'experimental_rerun'):
                st.experimental_rerun()
            return True
        
        # Debug: Print connection attempt
        st.write("🔧 Attempting to connect to Simple Kafka server...")
        
        # Generate and send messages using REST API
        generated_count = 0
        
        # Enhanced realistic data generation for live mode
        # Realistic first and last names
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'James', 'Olivia']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
        
        # Realistic product categories and names
        product_categories = {
            'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Smart Watch'],
            'Clothing': ['T-Shirt', 'Jeans', 'Sweater', 'Jacket', 'Dress'],
            'Books': ['Novel', 'Biography', 'Self-Help', 'Cookbook', 'Science Fiction'],
            'Home': ['Furniture', 'Kitchenware', 'Bedding', 'Decor', 'Tools']
        }
        
        for i in range(20):
            # Generate realistic customer
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            customer_id = 1000 + (generated_count * 10) + i
            
            # Generate realistic order amount with proper distribution
            price_ranges = [
                (10.99, 49.99, 0.5),   # 50% under $50
                (50.00, 199.99, 0.3),  # 30% $50-$200
                (200.00, 499.99, 0.15), # 15% $200-$500
                (500.00, 2999.99, 0.05), # 5% $500-$3000
            ]
            price_range = random.choices(
                [r[0:2] for r in price_ranges], 
                weights=[r[2] for r in price_ranges]
            )[0]
            total_amount = round(random.uniform(*price_range), 2)
            
            # Occasionally generate high-value orders (potential fraud)
            if random.random() < 0.08:
                total_amount = round(random.uniform(3000, 15000), 2)
            
            order = {
                'event_type': 'order',
                'event_timestamp': datetime.now().isoformat(),
                'order_id': 2000 + generated_count * 20 + i,
                'customer_id': customer_id,
                'order_date': datetime.now().strftime('%Y-%m-%d'),
                'status': random.choices(
                    ['pending', 'processing', 'completed', 'cancelled', 'refunded'],
                    weights=[0.15, 0.25, 0.5, 0.08, 0.02]
                )[0],
                'total_amount': total_amount,
                'payment_method': random.choices(
                    ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'apple_pay'],
                    weights=[0.4, 0.3, 0.15, 0.1, 0.05]
                )[0],
                'shipping_address': f'{random.randint(100, 9999)} Main St, {random.choice(["New York", "Los Angeles", "Chicago"])}',
                'source_system': 'demo_data'
            }
            
            # Send the order using REST API
            response = requests.post(f"{SERVER_URL}/produce/ecommerce_orders", json={'value': order})
            if response.status_code == 200:
                generated_count += 1
            
            # Generate customer event (20% chance)
            if random.random() < 0.2:
                customer = {
                    'event_type': 'customer_create',
                    'customer_id': customer_id,
                    'name': f"{first_name} {last_name}",
                    'email': f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@example.com",
                    'customer_segment': random.choices(
                        ['Bronze', 'Silver', 'Gold', 'Platinum'],
                        weights=[0.5, 0.3, 0.15, 0.05]
                    )[0],
                    'event_timestamp': datetime.now().isoformat()
                }
                response = requests.post(f"{SERVER_URL}/produce/ecommerce_customers", json={'value': customer})
                if response.status_code == 200:
                    generated_count += 1
            
            # Generate order items (1-5 per order)
            num_items = random.randint(1, 5)
            remaining_amount = total_amount
            
            for j in range(num_items):
                # Select random product category and name
                category = random.choice(list(product_categories.keys()))
                product_type = random.choice(product_categories[category])
                
                # Generate realistic product name
                adjectives = ['Premium', 'Modern', 'Wireless', 'Smart', 'Durable']
                if random.random() < 0.3:
                    product_name = f"{random.choice(adjectives)} {product_type}"
                else:
                    product_name = product_type
                
                # Calculate item price
                if j == num_items - 1:
                    # Last item takes remaining amount
                    item_total = remaining_amount
                    quantity = max(1, int(item_total / random.uniform(10, 100)))
                    unit_price = round(item_total / quantity, 2)
                else:
                    # Random portion of remaining amount
                    max_portion = remaining_amount * 0.8
                    item_total = round(random.uniform(10, min(max_portion, 500)), 2)
                    quantity = random.randint(1, 3)
                    unit_price = round(item_total / quantity, 2)
                    remaining_amount -= item_total
                
                item = {
                    'event_type': 'order_item',
                    'event_timestamp': datetime.now().isoformat(),
                    'item_id': 3000 + generated_count * 50 + i * 5 + j,
                    'order_id': order['order_id'],
                    'product_name': product_name,
                    'category': category,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'total_price': round(unit_price * quantity, 2)
                }
                response = requests.post(f"{SERVER_URL}/produce/ecommerce_order_items", json={'value': item})
                if response.status_code == 200:
                    generated_count += 1
            
            # Generate fraud alert (15% chance for any order, higher for high-value)
            fraud_chance = 0.15
            if order['total_amount'] > 2000:
                fraud_chance = 0.4
            elif order['total_amount'] > 5000:
                fraud_chance = 0.7
            
            if random.random() < fraud_chance:
                fraud_score = min(5, max(3, int(order['total_amount'] / 1000)))
                fraud_reasons = []
                if order['total_amount'] > 5000:
                    fraud_reasons.append('High transaction amount')
                if order['status'] in ['cancelled', 'refunded']:
                    fraud_reasons.append('Suspicious order status')
                if random.random() < 0.1:
                    fraud_reasons.append('Unusual purchasing pattern')
                
                alert = {
                    'event_type': 'fraud_alert',
                    'event_timestamp': datetime.now().isoformat(),
                    'alert_id': f'FRAUD_{order["order_id"]}_{int(time.time())}',
                    'order_id': order['order_id'],
                    'customer_id': order['customer_id'],
                    'fraud_score': fraud_score,
                    'risk_level': 'HIGH' if fraud_score >= 4 else 'MEDIUM',
                    'reasons': fraud_reasons if fraud_reasons else ['Suspicious pattern'],
                    'total_amount': order['total_amount'],
                    'status': order['status'],
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
        # Get generation count to generate dynamic message counts
        generation_count = st.session_state.get('data_generation_count', 0)
        
        # Calculate dynamic message counts based on generation count
        # Base message counts with generation multiplier
        base_orders = 156 + (generation_count * 20)
        base_customers = 42 + (generation_count * 5)
        base_order_items = 312 + (generation_count * 40)
        base_fraud_alerts = 23 + (generation_count * 3)
        
        total_messages = base_orders + base_customers + base_order_items + base_fraud_alerts
        
        return {
            'server_running': True,
            'topics': {
                'ecommerce_orders': {
                    'partition_count': 3,
                    'total_messages': base_orders,
                    'partitions': {
                        0: {'messages': base_orders // 3, 'latest_offset': (base_orders // 3) - 1},
                        1: {'messages': base_orders // 3, 'latest_offset': (base_orders // 3) - 1},
                        2: {'messages': base_orders - (2 * (base_orders // 3)), 'latest_offset': (base_orders - (2 * (base_orders // 3))) - 1}
                    }
                },
                'ecommerce_customers': {
                    'partition_count': 2,
                    'total_messages': base_customers,
                    'partitions': {
                        0: {'messages': base_customers // 2, 'latest_offset': (base_customers // 2) - 1},
                        1: {'messages': base_customers - (base_customers // 2), 'latest_offset': (base_customers - (base_customers // 2)) - 1}
                    }
                },
                'ecommerce_order_items': {
                    'partition_count': 3,
                    'total_messages': base_order_items,
                    'partitions': {
                        0: {'messages': base_order_items // 3, 'latest_offset': (base_order_items // 3) - 1},
                        1: {'messages': base_order_items // 3, 'latest_offset': (base_order_items // 3) - 1},
                        2: {'messages': base_order_items - (2 * (base_order_items // 3)), 'latest_offset': (base_order_items - (2 * (base_order_items // 3))) - 1}
                    }
                },
                'ecommerce_fraud_alerts': {
                    'partition_count': 1,
                    'total_messages': base_fraud_alerts,
                    'partitions': {
                        0: {'messages': base_fraud_alerts, 'latest_offset': base_fraud_alerts - 1}
                    }
                }
            },
            'total_messages': total_messages
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
    
    # Import necessary modules inside function to avoid Streamlit import issues
    import random
    from datetime import datetime, timedelta
    
    # Generate demo data if in demo mode
    if hasattr(st, 'session_state') and st.session_state.get('demo_mode', False):
        
        # Use data generation count to seed randomness for consistent new data each time
        generation_count = st.session_state.get('data_generation_count', 0)
        
        # Apply random seed with generation count to ensure different data each time
        # Also use a unique seed for each topic to ensure topic-specific randomness
        topic_seed = hash(f"{generation_count}_{topic}") % (2**32)
        random.seed(topic_seed)
        
        # Enhanced realistic data generation
        
        # Realistic first and last names for customers
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'James', 'Olivia',
                      'Robert', 'Ava', 'William', 'Sophia', 'Joseph', 'Isabella', 'Thomas', 'Mia',
                      'Charles', 'Charlotte', 'Daniel', 'Amelia', 'Matthew', 'Harper', 'Anthony', 'Evelyn',
                      'Donald', 'Abigail', 'Mark', 'Emily', 'Paul', 'Elizabeth', 'Andrew', 'Sofia',
                      'Joshua', 'Avery', 'Kevin', 'Ella', 'Brian', 'Scarlett', 'George', 'Grace',
                      'Edward', 'Chloe', 'Ryan', 'Victoria', 'Jason', 'Riley', 'Jeff', 'Lily',
                      'Gary', 'Madison', 'Kevin', 'Eleanor', 'Tim', 'Hannah', 'Steve', 'Layla']
        
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                     'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas',
                     'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White',
                     'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young',
                     'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
                     'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
                     'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker']
        
        # Realistic product categories and names
        product_categories = {
            'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Smart Watch', 'Camera',
                           'Bluetooth Speaker', 'Gaming Console', 'Monitor', 'Keyboard', 'Mouse', 'Printer'],
            'Clothing': ['T-Shirt', 'Jeans', 'Sweater', 'Jacket', 'Dress', 'Shirt', 'Pants', 'Shorts',
                        'Skirt', 'Blouse', 'Socks', 'Shoes'],
            'Books': ['Novel', 'Biography', 'Self-Help', 'Cookbook', 'Textbook', 'Science Fiction',
                     'Mystery', 'Romance', 'History', 'Childrens Book', 'Poetry', 'Business'],
            'Home': ['Furniture', 'Kitchenware', 'Bedding', 'Decor', 'Cleaning Supplies', 'Tools',
                    'Appliances', 'Gardening', 'Storage', 'Lighting', 'Bathroom', 'Outdoor']
        }
        
        # Generate synthetic data based on topic
        if topic == 'ecommerce_orders':
            # Generate sample orders
            orders = []
            now = datetime.now()
            
            # Generate more orders with realistic IDs
            for i in range(100):
                # Vary the time distribution (more recent orders are more frequent)
                if i < 20:
                    # Most recent 20 orders (last 30 minutes)
                    order_date = now - timedelta(minutes=random.randint(0, 30))
                elif i < 50:
                    # Next 30 orders (30 min to 2 hours)
                    order_date = now - timedelta(minutes=random.randint(30, 120))
                else:
                    # Older orders (2 hours to 24 hours)
                    order_date = now - timedelta(minutes=random.randint(120, 1440))
                
                # Realistic price ranges with occasional outliers
                price_ranges = [
                    (10.99, 49.99, 0.5),   # 50% of orders are under $50
                    (50.00, 199.99, 0.3),  # 30% between $50-$200
                    (200.00, 499.99, 0.15), # 15% between $200-$500
                    (500.00, 1999.99, 0.04), # 4% between $500-$2000
                    (2000.00, 8000.00, 0.01) # 1% over $2000 (potential fraud)
                ]
                
                # Choose price range based on weights
                price_range = random.choices(
                    [r[0:2] for r in price_ranges], 
                    weights=[r[2] for r in price_ranges]
                )[0]
                total_amount = round(random.uniform(*price_range), 2)
                
                # Generate realistic order ID (random 9-digit number)
                order_id = random.randint(100000000, 999999999)
                
                # Customer ID (random 6-digit number)
                customer_id = random.randint(100000, 999999)
                
                # Realistic payment methods with proper distribution
                payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'apple_pay', 'google_pay']
                payment_weights = [0.4, 0.3, 0.15, 0.08, 0.04, 0.03]
                payment_method = random.choices(payment_methods, weights=payment_weights)[0]
                
                # Realistic order status distribution
                order_statuses = ['pending', 'processing', 'completed', 'cancelled', 'refunded']
                status_weights = [0.15, 0.25, 0.5, 0.08, 0.02]
                status = random.choices(order_statuses, weights=status_weights)[0]
                
                order = {
                    'event_type': 'order',
                    'event_timestamp': order_date.isoformat(),
                    'order_id': order_id,
                    'customer_id': customer_id,
                    'order_date': order_date.strftime('%Y-%m-%d'),
                    'status': status,
                    'total_amount': total_amount,
                    'payment_method': payment_method,
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
            
            for i in range(50):
                # Generate realistic customer ID
                customer_id = random.randint(100000, 999999)
                
                # Generate realistic name
                first_name = random.choice(first_names)
                last_name = random.choice(last_names)
                full_name = f"{first_name} {last_name}"
                
                # Generate realistic email
                email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@example.com"
                
                # Realistic customer segments with proper distribution
                segments = ['Bronze', 'Silver', 'Gold', 'Platinum']
                segment_weights = [0.5, 0.3, 0.15, 0.05]
                customer_segment = random.choices(segments, weights=segment_weights)[0]
                
                # Event type distribution (more creates than updates)
                event_types = ['customer_create', 'customer_update']
                event_weights = [0.7, 0.3]
                event_type = random.choices(event_types, weights=event_weights)[0]
                
                # Vary timestamp (newer customers are more frequent)
                if i < 10:
                    # Recent customers (last 3 days)
                    event_date = now - timedelta(days=random.randint(0, 3))
                elif i < 30:
                    # 3-30 days
                    event_date = now - timedelta(days=random.randint(3, 30))
                else:
                    # Older customers (30-365 days)
                    event_date = now - timedelta(days=random.randint(30, 365))
                
                customer = {
                    'event_type': event_type,
                    'event_timestamp': event_date.isoformat(),
                    'customer_id': customer_id,
                    'name': full_name,
                    'email': email,
                    'customer_segment': customer_segment,
                    'first_name': first_name,
                    'last_name': last_name,
                    '_kafka_timestamp': event_date,
                    '_kafka_offset': i,
                    '_kafka_partition': i % 2
                }
                customers.append(customer)
            
            df = pd.DataFrame(customers)
            
        elif topic == 'ecommerce_order_items':
            # Generate sample order items
            order_items = []
            now = datetime.now()
            
            # Create a list of order IDs to reference
            order_ids = [random.randint(100000000, 999999999) for _ in range(50)]
            
            for i in range(200):
                # Get a random order ID
                order_id = random.choice(order_ids)
                
                # Item ID (random 8-digit number)
                item_id = random.randint(10000000, 99999999)
                
                # Random category
                category = random.choice(list(product_categories.keys()))
                
                # Random product type from category
                product_type = random.choice(product_categories[category])
                
                # Generate realistic product name
                adjectives = ['Premium', 'Deluxe', 'Classic', 'Modern', 'Vintage', 'Organic',
                            'Eco-Friendly', 'Smart', 'Wireless', 'Noise-Canceling', 'Waterproof', 'Durable']
                if random.random() < 0.3:
                    # Add an adjective to 30% of product names
                    product_name = f"{random.choice(adjectives)} {product_type}"
                else:
                    product_name = product_type
                
                # Realistic quantities (most items are 1-3, occasional higher quantities)
                if random.random() < 0.05:
                    # 5% of items have higher quantities (4-10)
                    quantity = random.randint(4, 10)
                else:
                    quantity = random.randint(1, 3)
                
                # Realistic unit prices based on category
                category_prices = {
                    'Electronics': (19.99, 999.99),
                    'Clothing': (9.99, 199.99),
                    'Books': (4.99, 49.99),
                    'Home': (14.99, 499.99)
                }
                min_price, max_price = category_prices[category]
                unit_price = round(random.uniform(min_price, max_price), 2)
                
                # Calculate total price
                total_price = round(unit_price * quantity, 2)
                
                # Vary timestamp
                item_date = now - timedelta(minutes=random.randint(0, 1440))
                
                order_items.append({
                    'event_type': 'order_item',
                    'event_timestamp': item_date.isoformat(),
                    'item_id': item_id,
                    'order_id': order_id,
                    'product_name': product_name,
                    'category': category,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'total_price': total_price,
                    '_kafka_timestamp': item_date,
                    '_kafka_offset': i,
                    '_kafka_partition': i % 3
                })
            
            df = pd.DataFrame(order_items)
            
        elif topic == 'ecommerce_fraud_alerts':
            # Generate sample fraud alerts
            fraud_alerts = []
            now = datetime.now()
            
            for i in range(25):
                # Get a realistic order ID
                order_id = random.randint(100000000, 999999999)
                
                # Customer ID
                customer_id = random.randint(100000, 999999)
                
                # Generate realistic fraud reasons
                fraud_reasons = [
                    'High transaction amount',
                    'Unusual purchasing pattern',
                    'Multiple orders in short time',
                    'Suspicious payment method',
                    'High-risk location',
                    'New customer with large order',
                    'Suspicious IP address',
                    'Failed payment attempts'
                ]
                
                # Select 1-3 fraud reasons
                num_reasons = random.randint(1, 3)
                selected_reasons = random.sample(fraud_reasons, num_reasons)
                
                # Generate realistic total amount for fraud cases
                total_amount = round(random.uniform(2000, 15000), 2)
                
                # Calculate fraud score based on amount and reasons
                base_score = min(5, max(3, int(total_amount / 1000)))
                reason_bonus = len(selected_reasons) - 1
                fraud_score = min(5, base_score + reason_bonus)
                
                # Risk level based on score
                risk_level = 'HIGH' if fraud_score >= 4 else 'MEDIUM'
                
                # Requires review if high risk
                requires_review = fraud_score >= 4
                
                # Vary timestamp (spread over last 24 hours)
                alert_date = now - timedelta(minutes=random.randint(0, 1440))
                
                fraud_alerts.append({
                    'event_type': 'fraud_alert',
                    'event_timestamp': alert_date.isoformat(),
                    'alert_id': f'FRAUD_{order_id}_{int(time.time())}',
                    'order_id': order_id,
                    'customer_id': customer_id,
                    'fraud_score': fraud_score,
                    'risk_level': risk_level,
                    'reasons': selected_reasons,
                    'total_amount': total_amount,
                    'requires_review': requires_review,
                    '_kafka_timestamp': alert_date,
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
        
        # Try to initialize integrated server only if Flask is available
        if st.button("🚀 Initialize Simple Kafka Server"):
            if not flask_available:
                st.error("❌ Cannot start server: Flask dependency not available")
                st.info("Demo mode works without Flask. Please enable it in settings.")
                return
            
            try:
                with st.spinner("Starting Kafka server..."):
                    if start_kafka_server_in_background():
                        st.success("✅ Server initialized successfully!")
                        st.rerun()
                    else:
                        st.warning("⚠️ Server already initialized")
            except Exception as e:
                st.error(f"Failed to initialize server: {e}")
                import traceback
                st.code(traceback.format_exc())
        
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
        server_mode = "Integrated Mode" if flask_available else "Demo Mode"
        st.info(f"ℹ️ {server_mode}")
        st.write("Kafka Server Embedded" if flask_available else "Using Synthetic Data")
    
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
    
    # Initialize data generation counter
    if 'data_generation_count' not in st.session_state:
        st.session_state.data_generation_count = 0
    
    st.title("⚡ Simple Kafka Real-Time Dashboard")
    st.markdown("""
    **Live Data Pipeline Dashboard** - Real-time analytics from Simple Kafka in-memory broker
    
    This dashboard provides:
    - **Server Status** - Monitor Simple Kafka broker and topics
    - **Real-Time Metrics** - Live revenue, orders, and customer analytics  
    - **Fraud Detection** - Suspicious pattern identification
    - **Order Stream** - Recent orders with filtering capabilities
    """)
    
    # Start Kafka server automatically when not in demo mode and Flask is available
    if not st.session_state.demo_mode:
        if flask_available:
            with st.spinner("Initializing Simple Kafka Server..."):
                start_kafka_server_in_background()
        else:
            st.warning("⚠️ Live mode requires Flask. Please enable demo mode instead.")
            # Auto-switch to demo mode if Flask is not available
            st.session_state.demo_mode = True
    
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