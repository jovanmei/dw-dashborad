"""
Configuration module for the e-commerce ETL project.

In a real project these values would typically come from environment
variables or an external config file. For this homework-style project
we keep them in code for simplicity while still demonstrating good
structure.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "EcommerceETL"
    shuffle_partitions: int = 4


@dataclass(frozen=True)
class Paths:
    """
    Logical locations for inputs/outputs. Replace with real lake/warehouse
    locations in production.
    """

    # Raw inputs (for demo, you can drop small CSV / JSON / Parquet files here)
    raw_orders: str = "data/raw/orders.csv"
    raw_customers: str = "data/raw/customers.json"
    raw_order_items: str = "data/raw/order_items.csv"
    raw_products: str = "data/raw/products.parquet"

    # Bronze (raw ingested to lake)
    bronze_base: str = "lake/bronze"

    # Silver (cleaned, modeled)
    silver_base: str = "lake/silver"

    # Gold (aggregated, dashboard-ready)
    gold_base: str = "lake/gold"

    # Legacy curated outputs (still used in code)
    customer_metrics: str = "output/customer_metrics"
    fact_orders: str = "output/fact_orders"


SPARK_CONFIG = SparkConfig()
PATHS = Paths()


