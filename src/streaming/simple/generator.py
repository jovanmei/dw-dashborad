"""
Enhanced data generator for Simple Kafka - populates all topics.

This generator creates realistic e-commerce data across all topics:
- ecommerce_orders: Order events
- ecommerce_customers: Customer updates  
- ecommerce_order_items: Order line items
- ecommerce_fraud_alerts: Fraud detection alerts
"""

import json
import time
import random
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from src.streaming.simple.server import SimpleKafkaProducer
except ImportError:
    # This should now work because of the sys.path injection
    from src.streaming.simple.server import SimpleKafkaProducer


class EnhancedDataGenerator:
    """Enhanced data generator that populates all Kafka topics."""
    
    def __init__(self):
        self.producer = SimpleKafkaProducer(
            bootstrap_servers='http://localhost:5051',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Data tracking
        self.order_id = 1000
        self.customer_id = 101
        self.item_id = 1
        self.customers_cache = {}  # Cache customer data
        self.products = self._generate_products()
        
        # Statistics
        self.stats = {
            'orders': 0,
            'customers': 0,
            'order_items': 0,
            'fraud_alerts': 0
        }
    
    def _generate_products(self) -> List[Dict]:
        """Generate a catalog of realistic products."""
        
        # Enhanced realistic product categories and names
        product_categories = {
            'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Smart Watch', 'Camera',
                           'Bluetooth Speaker', 'Gaming Console', 'Monitor', 'Keyboard', 'Mouse', 'Printer'],
            'Clothing': ['T-Shirt', 'Jeans', 'Sweater', 'Jacket', 'Dress', 'Shirt', 'Pants', 'Shorts',
                        'Skirt', 'Blouse', 'Socks', 'Shoes'],
            'Books': ['Novel', 'Biography', 'Self-Help', 'Cookbook', 'Textbook', 'Science Fiction',
                     'Mystery', 'Romance', 'History', 'Children\'s Book', 'Poetry', 'Business'],
            'Home': ['Furniture', 'Kitchenware', 'Bedding', 'Decor', 'Cleaning Supplies', 'Tools',
                    'Appliances', 'Gardening', 'Storage', 'Lighting', 'Bathroom', 'Outdoor'],
            'Sports': ['Running Shoes', 'Yoga Mat', 'Dumbbells', 'Soccer Ball', 'Basketball', 'Tennis Racket',
                      'Golf Clubs', 'Fitness Tracker', 'Water Bottle', 'Sports Bag', 'Helmet', 'Bike'],
            'Beauty': ['Skincare Set', 'Makeup Kit', 'Perfume', 'Shampoo', 'Conditioner', 'Moisturizer',
                      'Sunscreen', 'Lipstick', 'Foundation', 'Mascara', 'Face Mask', 'Body Lotion']
        }
        
        products = []
        adjectives = ['Premium', 'Deluxe', 'Classic', 'Modern', 'Vintage', 'Organic',
                    'Eco-Friendly', 'Smart', 'Wireless', 'Noise-Canceling', 'Waterproof', 'Durable']
        
        for i in range(50):
            # Get random category
            category = random.choice(list(product_categories.keys()))
            
            # Get random product type from category
            product_type = random.choice(product_categories[category])
            
            # Generate realistic product name
            if random.random() < 0.3:
                # Add an adjective to 30% of product names
                product_name = f"{random.choice(adjectives)} {product_type}"
            else:
                product_name = product_type
            
            # Realistic prices based on category
            category_prices = {
                'Electronics': (19.99, 999.99),
                'Clothing': (9.99, 199.99),
                'Books': (4.99, 49.99),
                'Home': (14.99, 499.99),
                'Sports': (19.99, 299.99),
                'Beauty': (9.99, 149.99)
            }
            min_price, max_price = category_prices[category]
            price = round(random.uniform(min_price, max_price), 2)
            
            products.append({
                'product_id': f'PROD_{i+1:03d}',
                'name': product_name,
                'category': category,
                'price': price
            })
        
        return products
    
    def generate_customer_event(self) -> Dict:
        """Generate a customer event with realistic customer data."""
        
        # Realistic first and last names
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
        
        # Sometimes update existing customer, sometimes create new
        if random.random() < 0.3 and self.customers_cache:
            # Update existing customer
            customer_id = random.choice(list(self.customers_cache.keys()))
            customer = self.customers_cache[customer_id].copy()
            customer['event_type'] = 'customer_update'
            customer['last_updated'] = datetime.now().isoformat()
        else:
            # Create new customer
            customer_id = self.customer_id
            self.customer_id += 1
            
            # Generate realistic name
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            full_name = f"{first_name} {last_name}"
            
            # Generate realistic email
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@example.com"
            
            # Generate realistic phone number
            area_code = random.choice(['202', '213', '312', '713', '602', '305', '415', '773', '212', '303'])
            phone = f'+1-{area_code}-{random.randint(100, 999)}-{random.randint(1000, 9999)}'
            
            # Realistic streets
            streets = ['Main St', 'Maple Ave', 'Oak St', 'Cedar Rd', 'Pine Ln', 'Elm St',
                      'Birch Ave', 'Walnut St', 'Chestnut Rd', 'Ash Ln', 'Spruce St', 'Willow Ave']
            street = random.choice(streets)
            
            # Realistic customer segments with proper distribution
            segments = ['Bronze', 'Silver', 'Gold', 'Platinum']
            segment_weights = [0.5, 0.3, 0.15, 0.05]
            customer_segment = random.choices(segments, weights=segment_weights)[0]
            
            customer = {
                'event_type': 'customer_create',
                'customer_id': customer_id,
                'name': full_name,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'address': f'{random.randint(100, 9999)} {street}',
                'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
                                      'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Francisco']),
                'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'FL', 'OH', 'GA', 'NC']),
                'zip_code': f'{random.randint(10000, 99999)}',
                'registration_date': datetime.now().strftime('%Y-%m-%d'),
                'customer_segment': customer_segment,
                'event_timestamp': datetime.now().isoformat()
            }
            
            # Cache the customer
            self.customers_cache[customer_id] = customer
        
        return customer
    
    def generate_order_event(self) -> Dict:
        """Generate an order event."""
        # Ensure we have some customers
        if not self.customers_cache:
            self.generate_customer_event()
        
        customer_id = random.choice(list(self.customers_cache.keys()))
        order_id = self.order_id
        self.order_id += 1
        
        # Generate order amount
        total_amount = round(random.uniform(10.99, 2999.99), 2)
        
        # Occasionally generate high-value orders (potential fraud)
        if random.random() < 0.08:
            total_amount = round(random.uniform(3000, 15000), 2)
        
        # Order status distribution
        status_weights = {
            'pending': 0.2,
            'processing': 0.3,
            'completed': 0.4,
            'cancelled': 0.08,
            'refunded': 0.02
        }
        status = random.choices(
            list(status_weights.keys()), 
            weights=list(status_weights.values())
        )[0]
        
        order = {
            'event_type': 'order',
            'event_timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': datetime.now().strftime('%Y-%m-%d'),
            'status': status,
            'total_amount': total_amount,
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
            'shipping_address': self.customers_cache[customer_id].get('address', 'Unknown'),
            'source_system': 'simple_kafka'
        }
        
        return order
    
    def generate_order_items(self, order_id: int, total_amount: float) -> List[Dict]:
        """Generate order items for an order."""
        # Determine number of items (1-5)
        num_items = random.randint(1, 5)
        items = []
        remaining_amount = total_amount
        
        for i in range(num_items):
            product = random.choice(self.products)
            
            # For last item, use remaining amount
            if i == num_items - 1:
                item_total = remaining_amount
                quantity = max(1, int(item_total / product['price']))
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
                'item_id': self.item_id,
                'order_id': order_id,
                'product_id': product['product_id'],
                'product_name': product['name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': round(unit_price * quantity, 2)
            }
            
            items.append(item)
            self.item_id += 1
        
        return items
    
    def generate_fraud_alert(self, order: Dict) -> Dict:
        """Generate a fraud alert based on order characteristics."""
        fraud_score = 0
        reasons = []
        
        # High amount
        if order['total_amount'] > 5000:
            fraud_score += 4
            reasons.append('High transaction amount')
        elif order['total_amount'] > 2000:
            fraud_score += 2
            reasons.append('Elevated transaction amount')
        
        # Suspicious status
        if order['status'] in ['cancelled', 'refunded']:
            fraud_score += 1
            reasons.append('Suspicious order status')
        
        # Random additional risk factors
        if random.random() < 0.1:
            fraud_score += 2
            reasons.append('Unusual customer behavior pattern')
        
        if random.random() < 0.05:
            fraud_score += 3
            reasons.append('Payment method risk')
        
        # Only create alert if score is significant
        if fraud_score >= 3:
            alert = {
                'event_type': 'fraud_alert',
                'event_timestamp': datetime.now().isoformat(),
                'alert_id': f'FRAUD_{order["order_id"]}_{int(time.time())}',
                'order_id': order['order_id'],
                'customer_id': order['customer_id'],
                'fraud_score': fraud_score,
                'risk_level': 'HIGH' if fraud_score >= 5 else 'MEDIUM',
                'reasons': reasons,
                'total_amount': order['total_amount'],
                'status': order['status'],
                'requires_review': fraud_score >= 5
            }
            return alert
        
        return None
    
    def generate_and_send_events(self):
        """Generate and send events to all topics."""
        # Generate customer event (20% chance)
        if random.random() < 0.2:
            customer_event = self.generate_customer_event()
            self.producer.send('ecommerce_customers', customer_event)
            self.stats['customers'] += 1
        
        # Generate order event (always)
        order_event = self.generate_order_event()
        self.producer.send('ecommerce_orders', order_event)
        self.stats['orders'] += 1
        
        # Generate order items for this order
        order_items = self.generate_order_items(
            order_event['order_id'], 
            order_event['total_amount']
        )
        
        for item in order_items:
            self.producer.send('ecommerce_order_items', item)
            self.stats['order_items'] += 1
        
        # Check for fraud alert (15% chance for any order, higher for high-value)
        fraud_chance = 0.15
        if order_event['total_amount'] > 2000:
            fraud_chance = 0.4
        elif order_event['total_amount'] > 5000:
            fraud_chance = 0.7
        
        if random.random() < fraud_chance:
            fraud_alert = self.generate_fraud_alert(order_event)
            if fraud_alert:
                self.producer.send('ecommerce_fraud_alerts', fraud_alert)
                self.stats['fraud_alerts'] += 1
    
    def print_stats(self):
        """Print generation statistics."""
        print(f"[STATS] Generated: Orders={self.stats['orders']}, "
              f"Customers={self.stats['customers']}, "
              f"Items={self.stats['order_items']}, "
              f"Fraud Alerts={self.stats['fraud_alerts']}")
    
    def close(self):
        """Close the producer."""
        self.producer.close()


def main():
    """Main function to run the enhanced data generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Simple Kafka Data Generator')
    parser.add_argument("--interval", type=float, default=2.0, 
                       help="Interval between events (seconds)")
    parser.add_argument("--duration", type=int, default=None,
                       help="Duration to run (seconds)")
    parser.add_argument("--burst", action="store_true",
                       help="Generate initial burst of data")
    args = parser.parse_args()
    
    generator = EnhancedDataGenerator()
    start_time = time.time()
    
    print("[START] Enhanced Simple Kafka Data Generator")
    print("=" * 50)
    print("Populating all topics:")
    print("  - ecommerce_orders")
    print("  - ecommerce_customers") 
    print("  - ecommerce_order_items")
    print("  - ecommerce_fraud_alerts")
    print(f"\nGenerating events every {args.interval}s...")
    
    try:
        # Initial burst to populate topics quickly
        if args.burst:
            print("\n[BURST] Generating initial burst of data...")
            for i in range(20):
                generator.generate_and_send_events()
                if i % 5 == 0:
                    generator.print_stats()
            print("[OK] Initial burst complete\n")
        
        # Continuous generation
        while True:
            if args.duration and (time.time() - start_time) > args.duration:
                break
            
            generator.generate_and_send_events()
            
            # Print stats every 10 events
            if generator.stats['orders'] % 10 == 0:
                generator.print_stats()
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n[STOP] Stopping generator...")
    finally:
        generator.print_stats()
        print(f"\n[FINAL] Final Statistics:")
        print(f"   Total Runtime: {time.time() - start_time:.1f}s")
        print(f"   Events/Second: {sum(generator.stats.values()) / (time.time() - start_time):.2f}")
        generator.close()


if __name__ == "__main__":
    main()