"""
Synthetic data generation for the e-commerce ETL pipeline.

For a resume project it's useful to show how you can mock upstream
systems (OLTP DB, CRM, product catalog) while keeping schema definitions
explicit and strongly typed.
"""

from __future__ import annotations

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
    Create small in-memory datasets that mimic realistic e‑commerce sources.

    Returns
    -------
    (orders, customers, order_items, products)
    """

    # Orders
    orders_data = [
        (1, 101, "2024-01-15", "completed", 150.00),
        (2, 102, "2024-01-16", "completed", 320.50),
        (3, 101, "2024-02-20", "completed", 89.99),
        (4, 103, "2024-03-10", "cancelled", 250.00),
        (5, 104, "2024-03-15", "completed", 450.75),
        (6, 102, "2024-04-01", "completed", 199.99),
        (7, 105, "2024-04-10", "pending", 75.50),
        (8, 101, "2024-05-05", "completed", 299.99),
        (9, 103, "2024-05-20", "completed", 180.00),
        (10, 106, "2024-06-01", "completed", 520.00),
        (11, 104, "2024-06-15", "completed", 399.99),
        (12, 107, "2024-07-01", "completed", 150.00),
        (13, 105, "2024-07-10", "completed", 225.50),
        (14, 108, "2024-08-05", "completed", 675.00),
        (15, 106, "2024-08-20", "cancelled", 300.00),
    ]

    orders_schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("order_date", StringType(), False),
            StructField("status", StringType(), False),
            StructField("total_amount", DoubleType(), False),
        ]
    )

    df_orders = spark.createDataFrame(orders_data, orders_schema).withColumn(
        "order_date", col("order_date").cast(DateType())
    )

    # Customers (acts like JSON CRM dump)
    customers_data = [
        {
            "customer_id": 101,
            "name": "Alice Johnson",
            "email": "alice@email.com",
            "join_date": "2023-12-01",
            "segment": "Premium",
        },
        {
            "customer_id": 102,
            "name": "Bob Smith",
            "email": "bob@email.com",
            "join_date": "2024-01-10",
            "segment": "Regular",
        },
        {
            "customer_id": 103,
            "name": "Carol White",
            "email": "carol@email.com",
            "join_date": "2023-11-15",
            "segment": "Regular",
        },
        {
            "customer_id": 104,
            "name": "David Brown",
            "email": "david@email.com",
            "join_date": "2024-02-20",
            "segment": "Premium",
        },
        {
            "customer_id": 105,
            "name": "Emma Davis",
            "email": "emma@email.com",
            "join_date": "2024-03-05",
            "segment": "Regular",
        },
        {
            "customer_id": 106,
            "name": "Frank Miller",
            "email": "frank@email.com",
            "join_date": "2024-05-01",
            "segment": "Premium",
        },
        {
            "customer_id": 107,
            "name": "Grace Lee",
            "email": "grace@email.com",
            "join_date": "2024-06-10",
            "segment": "Regular",
        },
        {
            "customer_id": 108,
            "name": "Henry Wilson",
            "email": "henry@email.com",
            "join_date": "2024-07-15",
            "segment": "Premium",
        },
    ]
    df_customers = spark.createDataFrame(customers_data).withColumn(
        "join_date", col("join_date").cast(DateType())
    )

    # Order items
    order_items_data = [
        (1, 1, 201, 2, 75.00),
        (2, 2, 202, 1, 320.50),
        (3, 3, 203, 1, 89.99),
        (4, 4, 201, 1, 250.00),
        (5, 5, 204, 3, 150.25),
        (6, 6, 202, 1, 199.99),
        (7, 7, 205, 2, 37.75),
        (8, 8, 203, 3, 99.99),
        (9, 9, 201, 2, 90.00),
        (10, 10, 204, 4, 130.00),
    ]

    order_items_schema = StructType(
        [
            StructField("item_id", IntegerType(), False),
            StructField("order_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("price", DoubleType(), False),
        ]
    )
    df_order_items = spark.createDataFrame(order_items_data, order_items_schema)

    # Products
    products_data = [
        (201, "Laptop", "Electronics", 750.00),
        (202, "Smartphone", "Electronics", 320.50),
        (203, "Headphones", "Electronics", 89.99),
        (204, "Coffee Maker", "Home", 150.25),
        (205, "Running Shoes", "Sports", 75.50),
    ]

    products_schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("unit_price", DoubleType(), False),
        ]
    )
    df_products = spark.createDataFrame(products_data, products_schema)

    return df_orders, df_customers, df_order_items, df_products


