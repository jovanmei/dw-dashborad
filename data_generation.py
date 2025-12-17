"""
Synthetic data generation for the e-commerce ETL pipeline.

This module generates realistic, DIRTY data that mimics real-world data quality issues
commonly found in production systems. Includes missing values, inconsistent formats,
duplicates, and other data quality challenges to demonstrate robust ETL handling.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def create_sample_dataframes(spark: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Create realistic, DIRTY datasets that mimic real-world e-commerce data quality issues.
    
    This function generates data with common problems found in production:
    - Missing/null values
    - Inconsistent date formats
    - Duplicate records
    - Invalid data (negative amounts, future dates)
    - Mixed case and typos
    - Orphaned records (referential integrity issues)
    
    Returns
    -------
    (orders, customers, order_items, products)
    """
    
    # Generate dirty orders data with realistic issues
    orders_data = _generate_dirty_orders_data()
    
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),  # Allow nulls
        StructField("customer_id", IntegerType(), True),  # Allow nulls
        StructField("order_date", StringType(), True),  # Keep as string to show inconsistent formats
        StructField("status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("source_system", StringType(), True),  # New field for data lineage
        StructField("last_updated", StringType(), True),  # New field with timestamp issues
    ])

    df_orders = spark.createDataFrame(orders_data, orders_schema)

    # Generate dirty customers data (JSON CRM dump with quality issues)
    customers_data = _generate_dirty_customers_data()
    
    # Define explicit schema for customers to handle mixed types
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("join_date", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("marketing_opt_in", StringType(), True),
        StructField("last_login", StringType(), True),
    ])
    
    df_customers = spark.createDataFrame(customers_data, customers_schema)

    # Generate dirty order items data
    order_items_data = _generate_dirty_order_items_data()
    
    order_items_schema = StructType([
        StructField("item_id", IntegerType(), True),  # Allow nulls
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),  # New field
        StructField("tax_amount", DoubleType(), True),  # New field
        StructField("line_total", DoubleType(), True),  # New field (may not match calculations)
    ])
    df_order_items = spark.createDataFrame(order_items_data, order_items_schema)

    # Generate dirty products data
    products_data = _generate_dirty_products_data()
    
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),  # New field
        StructField("unit_price", DoubleType(), True),
        StructField("cost", DoubleType(), True),  # New field
        StructField("supplier_id", IntegerType(), True),  # New field
        StructField("is_active", StringType(), True),  # New field (inconsistent boolean format)
        StructField("created_date", StringType(), True),  # New field
        StructField("weight_kg", DoubleType(), True),  # New field
    ])
    df_products = spark.createDataFrame(products_data, products_schema)

    return df_orders, df_customers, df_order_items, df_products




def _generate_dirty_orders_data():
    """Generate orders data with realistic quality issues."""
    
    # Base date for generating realistic date ranges
    base_date = datetime(2023, 1, 1)
    
    orders_data = []
    
    # Generate 150+ orders with various issues
    for i in range(1, 156):
        order_id = i
        
        # Introduce some null order_ids (data corruption)
        if i in [23, 67, 89]:
            order_id = None
            
        # Customer IDs with some orphaned records and nulls
        if i <= 50:
            customer_id = random.randint(101, 120)
        elif i <= 100:
            customer_id = random.randint(101, 150)
        elif i <= 140:
            customer_id = random.randint(101, 200)  # Some will be orphaned
        else:
            customer_id = None  # Some null customer_ids
            
        # Generate dates with inconsistent formats and some invalid dates
        days_offset = random.randint(0, 700)
        order_date = base_date + timedelta(days=days_offset)
        
        # Mix of date formats (real-world inconsistency)
        date_formats = [
            order_date.strftime("%Y-%m-%d"),      # ISO format
            order_date.strftime("%m/%d/%Y"),      # US format
            order_date.strftime("%d-%m-%Y"),      # European format
            order_date.strftime("%Y%m%d"),        # Compact format
            order_date.strftime("%B %d, %Y"),     # Long format
        ]
        
        if i % 15 == 0:  # Some null dates
            order_date_str = None
        elif i % 20 == 0:  # Some invalid dates
            order_date_str = "2024-13-45"  # Invalid date
        elif i % 25 == 0:  # Some future dates (data entry errors)
            future_date = datetime(2025, 6, 15)
            order_date_str = future_date.strftime("%Y-%m-%d")
        else:
            order_date_str = random.choice(date_formats)
            
        # Order status with inconsistent casing and typos
        statuses = [
            "completed", "Completed", "COMPLETED", "complete",  # Variations of completed
            "cancelled", "Cancelled", "CANCELLED", "canceled", "Canceled",  # Variations of cancelled
            "pending", "Pending", "PENDING", "processing", "Processing",
            "shipped", "Shipped", "delivered", "Delivered",
            "refunded", "Refunded", "returned", "failed",
            None,  # Some null statuses
            "unknwon",  # Typo
            "",  # Empty string
        ]
        status = random.choice(statuses)
        
        # Total amounts with various issues
        if i % 30 == 0:  # Some null amounts
            total_amount = None
        elif i % 40 == 0:  # Some negative amounts (refunds or data errors)
            total_amount = -random.uniform(10, 500)
        elif i % 50 == 0:  # Some zero amounts
            total_amount = 0.0
        elif i % 60 == 0:  # Some extremely high amounts (potential fraud or data errors)
            total_amount = random.uniform(10000, 50000)
        else:
            total_amount = round(random.uniform(5.99, 2999.99), 2)
            
        # Source system (new field for data lineage)
        source_systems = ["legacy_db", "new_api", "mobile_app", "web_portal", None, ""]
        source_system = random.choice(source_systems)
        
        # Last updated timestamp with inconsistent formats
        if i % 35 == 0:
            last_updated = None
        else:
            update_time = order_date + timedelta(hours=random.randint(0, 72))
            timestamp_formats = [
                update_time.strftime("%Y-%m-%d %H:%M:%S"),
                update_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                update_time.strftime("%m/%d/%Y %I:%M %p"),
                str(int(update_time.timestamp())),  # Unix timestamp
            ]
            last_updated = random.choice(timestamp_formats)
            
        orders_data.append((
            order_id, customer_id, order_date_str, status, 
            total_amount, source_system, last_updated
        ))
    
    # Add some duplicate orders (data quality issue)
    orders_data.append((1, 101, "2024-01-15", "completed", 150.00, "legacy_db", "2024-01-15 10:30:00"))
    orders_data.append((5, 104, "2024-03-15", "completed", 450.75, "new_api", "2024-03-15 14:22:33"))
    
    return orders_data


def _generate_dirty_customers_data():
    """Generate customers data with realistic CRM export quality issues."""
    
    customers_data = []
    
    # First names with various issues
    first_names = [
        "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry",
        "Isabella", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
        "Yara", "Zoe", "alex", "BETH", "charlie", "DIANA",  # Mixed case
        "José", "François", "Müller", "O'Connor",  # International names
        "", None, "Unknown", "N/A", "Test User",  # Invalid entries
    ]
    
    last_names = [
        "Johnson", "Smith", "White", "Brown", "Davis", "Miller", "Lee", "Wilson",
        "Garcia", "Martinez", "Anderson", "Taylor", "Thomas", "Jackson", "Harris",
        "martin", "CLARK", "lewis", "WALKER",  # Mixed case
        "O'Brien", "McDonald", "Van Der Berg", "De Silva",  # Complex names
        "", None, "Unknown", "TBD",  # Invalid entries
    ]
    
    # Generate 200+ customers
    for i in range(101, 301):
        customer_id = i
        
        # Some duplicate customer IDs (data quality issue)
        if i in [150, 175, 200]:
            customer_id = 101  # Duplicate of first customer
            
        # Some null customer IDs
        if i in [123, 167, 234]:
            customer_id = None
            
        # Generate names with various quality issues
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        if first_name and last_name:
            name_formats = [
                f"{first_name} {last_name}",
                f"{last_name}, {first_name}",  # Reversed format
                f"{first_name.upper()} {last_name.upper()}",  # All caps
                f"{first_name.lower()} {last_name.lower()}",  # All lowercase
                f"Mr. {first_name} {last_name}",  # With title
                f"Ms. {first_name} {last_name}",
                f"{first_name}",  # First name only
                f"{last_name}",   # Last name only
            ]
            name = random.choice(name_formats)
        else:
            name = None
            
        # Email addresses with various issues
        if i % 25 == 0:  # Some null emails
            email = None
        elif i % 30 == 0:  # Some invalid email formats
            invalid_emails = [
                "notanemail", "user@", "@domain.com", "user..name@domain.com",
                "user@domain", "user name@domain.com", "", "N/A", "TBD"
            ]
            email = random.choice(invalid_emails)
        elif first_name and last_name:
            # Generate realistic emails with some variations
            domains = ["gmail.com", "yahoo.com", "hotmail.com", "company.com", "email.com", "test.org"]
            email_formats = [
                f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}",
                f"{first_name.lower()}{last_name.lower()}@{random.choice(domains)}",
                f"{first_name.lower()}_{last_name.lower()}@{random.choice(domains)}",
                f"{first_name.lower()}{i}@{random.choice(domains)}",
                f"{first_name.lower()}.{last_name.lower()}.{random.randint(1,99)}@{random.choice(domains)}",
            ]
            email = random.choice(email_formats)
        else:
            email = f"user{i}@email.com"
            
        # Join dates with inconsistent formats
        if i % 20 == 0:  # Some null join dates
            join_date = None
        elif i % 35 == 0:  # Some invalid dates
            join_date = "2024-13-32"  # Invalid date
        else:
            # Generate dates between 2020 and 2024
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2024, 12, 31)
            days_diff = (end_date - start_date).days
            random_days = random.randint(0, days_diff)
            join_date_obj = start_date + timedelta(days=random_days)
            
            # Various date formats
            date_formats = [
                join_date_obj.strftime("%Y-%m-%d"),
                join_date_obj.strftime("%m/%d/%Y"),
                join_date_obj.strftime("%d-%m-%Y"),
                join_date_obj.strftime("%B %d, %Y"),
                join_date_obj.strftime("%Y%m%d"),
            ]
            join_date = random.choice(date_formats)
            
        # Customer segments with inconsistent values
        segments = [
            "Premium", "premium", "PREMIUM", "VIP", "Gold",  # Premium variations
            "Regular", "regular", "REGULAR", "Standard", "Basic", "Silver",  # Regular variations
            "Bronze", "New", "Inactive", "Churned", "Prospect",
            None, "", "Unknown", "TBD", "N/A",  # Invalid values
        ]
        segment = random.choice(segments)
        
        # Additional fields with quality issues
        phone_numbers = [
            f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}",
            f"555.{random.randint(100,999)}.{random.randint(1000,9999)}",
            f"({random.randint(100,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}",
            f"555{random.randint(1000000,9999999)}",  # No formatting
            "123-456-7890",  # Fake number
            None, "", "N/A", "Unknown"
        ]
        phone = random.choice(phone_numbers)
        
        # Address with missing components
        addresses = [
            f"{random.randint(1,9999)} Main St, City, ST {random.randint(10000,99999)}",
            f"{random.randint(1,999)} Oak Ave",  # Missing city/state
            f"Apt {random.randint(1,999)}, {random.randint(1,999)} Elm St, City, ST",  # Missing zip
            "123 Main Street, Anytown, USA",  # Generic address
            None, "", "Unknown", "TBD"
        ]
        address = random.choice(addresses)
        
        # Age with some invalid values (keep as integers, will be handled in schema)
        if i % 40 == 0:
            age = None
        elif i % 50 == 0:
            age = random.randint(-5, 5)  # Invalid negative or too young
        elif i % 60 == 0:
            age = random.randint(150, 200)  # Invalid too old
        else:
            age = random.randint(18, 85)
            
        customers_data.append({
            "customer_id": customer_id,
            "name": name,
            "email": email,
            "join_date": join_date,
            "segment": segment,
            "phone": phone,
            "address": address,
            "age": age,
            "marketing_opt_in": random.choice(["True", "False", None, "Yes", "No", "Y", "N", "1", "0", ""]),
            "last_login": random.choice([
                "2024-12-01", "2024-11-15T10:30:00Z", None, "", "Never", "2024-13-01"
            ])
        })
    
    return customers_data


def _generate_dirty_order_items_data():
    """Generate order items data with referential integrity and calculation issues."""
    
    order_items_data = []
    item_id = 1
    
    # Generate items for orders 1-155 (some orders may not have items - orphaned orders)
    for order_id in range(1, 156):
        # Skip some orders entirely (orphaned orders)
        if order_id % 25 == 0:
            continue
            
        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        
        for _ in range(num_items):
            # Some null item IDs
            current_item_id = item_id if item_id % 50 != 0 else None
            
            # Some null order IDs (referential integrity issues)
            current_order_id = order_id if order_id % 40 != 0 else None
            
            # Product IDs with some orphaned references
            product_id = random.randint(201, 250)  # Some products won't exist
            if item_id % 30 == 0:
                product_id = None
                
            # Quantities with issues
            if item_id % 45 == 0:
                quantity = None
            elif item_id % 55 == 0:
                quantity = 0  # Zero quantity
            elif item_id % 65 == 0:
                quantity = -1  # Negative quantity (returns?)
            else:
                quantity = random.randint(1, 10)
                
            # Prices with issues
            if item_id % 35 == 0:
                price = None
            elif item_id % 75 == 0:
                price = 0.0  # Zero price
            elif item_id % 85 == 0:
                price = -random.uniform(10, 100)  # Negative price
            else:
                price = round(random.uniform(5.99, 999.99), 2)
                
            # Discount percentage - ensure all values are floats
            if item_id % 20 == 0:
                discount_percent = None
            else:
                discount_choices = [0.0, 5.0, 15.0, 20.0, 25.0, 50.0, -5.0, 150.0]
                discount_percent = random.choice(discount_choices)
            
            # Tax amount (may not match calculations)
            if price and quantity:
                # Sometimes correct, sometimes wrong
                if item_id % 20 == 0:
                    tax_amount = None
                elif item_id % 30 == 0:
                    tax_amount = round(price * quantity * 0.08, 2)  # Correct 8% tax
                else:
                    tax_amount = round(random.uniform(0, price * quantity * 0.15), 2)  # Random tax
            else:
                tax_amount = None
                
            # Line total (may not match price * quantity calculations)
            if price and quantity:
                if item_id % 25 == 0:
                    line_total = None
                elif item_id % 35 == 0:
                    # Correct calculation
                    subtotal = price * quantity
                    discount_amount = subtotal * (discount_percent or 0.0) / 100.0
                    line_total = round(subtotal - discount_amount + (tax_amount or 0.0), 2)
                else:
                    # Incorrect calculation (data quality issue)
                    line_total = round(random.uniform(0, price * quantity * 1.5), 2)
            else:
                line_total = None
                
            order_items_data.append((
                current_item_id, 
                current_order_id, 
                product_id, 
                quantity,
                price,
                discount_percent,
                tax_amount,
                line_total
            ))
            
            item_id += 1
            
    # Add some duplicate item IDs (data quality issue) - ensure all values are proper types
    order_items_data.append((1, 1, 201, 2, 75.00, 15.0, 6.00, 81.00))
    order_items_data.append((5, 5, 204, 3, 150.25, 0.0, 12.02, 162.27))
    
    return order_items_data


def _generate_dirty_products_data():
    """Generate products data with catalog quality issues."""
    
    products_data = []
    
    # Product names with various quality issues
    product_names = [
        # Electronics
        "MacBook Pro 16\"", "iPhone 15 Pro", "Samsung Galaxy S24", "Dell XPS 13",
        "Sony WH-1000XM5", "AirPods Pro", "iPad Air", "Surface Pro 9",
        "LAPTOP", "laptop", "Laptop Pro Max Ultra", "",  # Quality issues
        
        # Home & Kitchen
        "Keurig K-Elite", "Ninja Foodi", "Instant Pot Duo", "Dyson V15",
        "KitchenAid Mixer", "Nespresso Vertuo", "Roomba i7+", "Air Fryer XL",
        "coffee maker", "BLENDER", "Unknown Product", None,  # Quality issues
        
        # Sports & Outdoors
        "Nike Air Max 270", "Adidas Ultraboost 22", "Fitbit Charge 5", "Garmin Forerunner",
        "Yeti Rambler", "Patagonia Jacket", "North Face Backpack", "REI Tent",
        "running shoes", "JACKET", "Product Name TBD", "",  # Quality issues
        
        # Books & Media
        "The Great Gatsby", "1984 by Orwell", "Harry Potter Set", "Kindle Paperwhite",
        "AirPods Case", "Phone Charger", "USB Cable", "Wireless Mouse",
        "book", "BOOK", "Item #12345", None,  # Quality issues
    ]
    
    # Categories with inconsistent naming
    categories = [
        "Electronics", "electronics", "ELECTRONICS", "Electronic", "Tech",
        "Home & Kitchen", "Home", "Kitchen", "home", "HOME_KITCHEN",
        "Sports & Outdoors", "Sports", "Outdoors", "sports", "SPORTS",
        "Books", "books", "BOOKS", "Media", "Book & Media",
        "", None, "Unknown", "TBD", "Miscellaneous", "Other"
    ]
    
    subcategories = [
        "Laptops", "Smartphones", "Headphones", "Tablets", "Accessories",
        "Coffee Makers", "Blenders", "Vacuum Cleaners", "Kitchen Appliances",
        "Running Shoes", "Fitness Trackers", "Outdoor Gear", "Apparel",
        "Fiction", "Non-Fiction", "E-Readers", "Cables & Chargers",
        "", None, "N/A", "TBD"
    ]
    
    # Generate 50+ products
    for i in range(201, 252):
        product_id = i
        
        # Some duplicate product IDs
        if i in [225, 235, 245]:
            product_id = 201
            
        # Some null product IDs
        if i in [220, 240]:
            product_id = None
            
        product_name = random.choice(product_names)
        category = random.choice(categories)
        subcategory = random.choice(subcategories)
        
        # Unit prices with issues
        if i % 20 == 0:
            unit_price = None
        elif i % 30 == 0:
            unit_price = 0.0  # Zero price
        elif i % 40 == 0:
            unit_price = -random.uniform(10, 100)  # Negative price
        elif i % 50 == 0:
            unit_price = random.uniform(50000, 100000)  # Unrealistic high price
        else:
            unit_price = round(random.uniform(9.99, 2999.99), 2)
            
        # Cost (should be less than unit_price, but may not be)
        if unit_price and unit_price > 0:
            if i % 25 == 0:
                cost = None
            elif i % 35 == 0:
                cost = unit_price * random.uniform(0.4, 0.8)  # Realistic cost
            else:
                cost = round(random.uniform(1, unit_price * 1.2), 2)  # May exceed price
        else:
            cost = None
            
        # Supplier IDs with some orphaned references
        supplier_id = random.choice([1001, 1002, 1003, 1004, 1005, 9999, None])
        
        # Is active with inconsistent boolean formats
        is_active = random.choice([
            "true", "false", "True", "False", "TRUE", "FALSE",
            "1", "0", "Y", "N", "Yes", "No", "Active", "Inactive",
            "", None
        ])
        
        # Created date with various formats
        if i % 30 == 0:
            created_date = None
        else:
            # Random date between 2020 and 2024
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2024, 12, 31)
            days_diff = (end_date - start_date).days
            random_days = random.randint(0, days_diff)
            created_date_obj = start_date + timedelta(days=random_days)
            
            date_formats = [
                created_date_obj.strftime("%Y-%m-%d"),
                created_date_obj.strftime("%m/%d/%Y"),
                created_date_obj.strftime("%Y-%m-%d %H:%M:%S"),
                created_date_obj.strftime("%Y%m%d"),
            ]
            created_date = random.choice(date_formats)
            
        # Weight with some invalid values
        if i % 25 == 0:
            weight_kg = None
        elif i % 45 == 0:
            weight_kg = 0.0  # Zero weight
        elif i % 55 == 0:
            weight_kg = -random.uniform(0.1, 5.0)  # Negative weight
        else:
            weight_kg = round(random.uniform(0.01, 50.0), 3)
            
        products_data.append((
            product_id, product_name, category, subcategory,
            unit_price, cost, supplier_id, is_active,
            created_date, weight_kg
        ))
    
    return products_data