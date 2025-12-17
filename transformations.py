"""
Core transformation logic for the e-commerce ETL pipeline.

This module contains reusable functions that can be unit-tested in
isolation from Spark job orchestration.
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_date,
    datediff,
    lag,
    max as _max,
    month,
    round as _round,
    sum as _sum,
    when,
    year,
)


def enrich_orders(df_orders: DataFrame) -> DataFrame:
    """
    Add derived attributes to the raw orders dataset.

    - order_year / order_month for partitioning and reporting
    - is_completed flag for faster filtering
    """

    return (
        df_orders.withColumn("order_year", year(col("order_date")))
        .withColumn("order_month", month(col("order_date")))
        .withColumn("is_completed", when(col("status") == "completed", 1).otherwise(0))
    )


def build_customer_metrics(
    df_orders_enriched: DataFrame, df_customers: DataFrame
) -> DataFrame:
    """
    Aggregate customer-level KPIs such as total revenue, order count and tenure.
    """

    customer_metrics = (
        df_orders_enriched.filter(col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
        )
        .withColumn("avg_order_value", _round(col("avg_order_value"), 2))
        .withColumn("total_revenue", _round(col("total_revenue"), 2))
    )

    customer_metrics = (
        customer_metrics.join(
            df_customers.select("customer_id", "join_date", "segment"), "customer_id"
        )
        .withColumn("tenure_days", datediff(current_date(), col("join_date")))
        .withColumn("tenure_days", col("tenure_days").cast("int"))
    )

    return customer_metrics


def build_fact_orders(
    df_orders_enriched: DataFrame,
    df_customers: DataFrame,
    df_order_items: DataFrame,
    df_products: DataFrame,
) -> DataFrame:
    """
    Build a denormalized fact table suitable for loading into a data warehouse.
    """

    fact_orders = (
        df_orders_enriched.join(df_customers, "customer_id", "left")
        .join(df_order_items, "order_id", "left")
        .join(df_products, "product_id", "left")
        .select(
            col("order_id"),
            col("customer_id"),
            col("name").alias("customer_name"),
            col("segment").alias("customer_segment"),
            col("order_date"),
            col("order_year"),
            col("order_month"),
            col("status"),
            col("total_amount"),
            col("product_id"),
            col("product_name"),
            col("category").alias("product_category"),
            col("quantity"),
            col("is_completed"),
        )
    )

    return fact_orders


def build_monthly_revenue(df_orders_enriched: DataFrame) -> DataFrame:
    """
    Compute monthly revenue trend for completed orders.
    """

    return (
        df_orders_enriched.filter(col("status") == "completed")
        .groupBy("order_year", "order_month")
        .agg(_sum("total_amount").alias("monthly_revenue"))
        .withColumn("monthly_revenue", _round(col("monthly_revenue"), 2))
        .orderBy("order_year", "order_month")
    )


def build_segment_analysis(fact_orders: DataFrame) -> DataFrame:
    """
    Summarise revenue and order volume by customer segment.
    """

    return (
        fact_orders.filter(col("status") == "completed")
        .groupBy("customer_segment")
        .agg(
            _sum("total_amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
        )
        .withColumn("total_revenue", _round(col("total_revenue"), 2))
        .withColumn("avg_order_value", _round(col("avg_order_value"), 2))
        .orderBy(col("total_revenue").desc())
    )


def build_status_distribution(df_orders: DataFrame) -> DataFrame:
    """
    Simple distribution of order status values.
    """

    return (
        df_orders.groupBy("status")
        .agg(count("order_id").alias("count"))
        .orderBy(col("count").desc())
    )


def build_category_performance(fact_orders: DataFrame) -> DataFrame:
    """
    Category-level revenue and units sold.
    """

    return (
        fact_orders.filter(col("is_completed") == 1)
        .groupBy("product_category")
        .agg(
            _sum(col("quantity") * col("total_amount")).alias("category_revenue"),
            _sum("quantity").alias("units_sold"),
        )
        .withColumn("category_revenue", _round(col("category_revenue"), 2))
        .orderBy(col("category_revenue").desc())
    )


def build_customer_rfm(fact_orders: DataFrame) -> DataFrame:
    """
    Derive simple RFM (recency, frequency, monetary) features and a
    high/medium/low value segment for each customer.

    Business value: helps marketing and CRM teams target high-value and
    at-risk customers with personalised campaigns.
    """

    completed = fact_orders.filter(col("status") == "completed")

    agg = (
        completed.groupBy("customer_id")
        .agg(
            _sum("total_amount").alias("monetary"),
            count("order_id").alias("frequency"),
            _max("order_date").alias("last_order_date"),
        )
        .withColumn(
            "recency_days", datediff(current_date(), col("last_order_date")).cast("int")
        )
    )

    # Very simple segmentation: high monetary & recent = high_value, etc.
    agg = agg.withColumn(
        "value_segment",
        when((col("monetary") >= 500) & (col("recency_days") <= 60), "high_value")
        .when((col("monetary") >= 250) & (col("recency_days") <= 90), "medium_value")
        .otherwise("low_value"),
    )

    return agg


def detect_monthly_revenue_anomalies(
    monthly_revenue: DataFrame, drop_threshold: float = 0.2
) -> DataFrame:
    """
    Flag months where revenue drops by more than `drop_threshold` vs the
    previous month.

    Business value: gives early warning about demand drops so commercial
    teams can investigate quickly.
    """

    window_spec = Window.orderBy("order_year", "order_month")

    with_prev = monthly_revenue.withColumn(
        "prev_month_revenue", lag("monthly_revenue").over(window_spec)
    )

    return with_prev.withColumn(
        "revenue_drop_pct",
        when(col("prev_month_revenue").isNull(), None).otherwise(
            (col("prev_month_revenue") - col("monthly_revenue"))
            / col("prev_month_revenue")
        ),
    ).filter(col("revenue_drop_pct") >= drop_threshold)


def build_customer_segment_monthly_dashboard(
    fact_orders: DataFrame, customer_rfm: DataFrame
) -> DataFrame:
    """
    Gold-layer dashboard table:

    One row per (year, month, customer_segment, value_segment) with
    revenue, order count and unique customer count. This is the kind of
    denormalised table BI tools (Power BI, Tableau) consume directly.
    """

    # Join value_segment onto fact_orders at customer level
    fact_with_value = fact_orders.join(customer_rfm.select("customer_id", "value_segment"), "customer_id")

    return (
        fact_with_value.filter(col("status") == "completed")
        .groupBy(
            "order_year",
            "order_month",
            "customer_segment",
            "value_segment",
        )
        .agg(
            _sum("total_amount").alias("revenue"),
            count("order_id").alias("order_count"),
            count("customer_id").alias("customer_count"),
        )
        .withColumn("revenue", _round(col("revenue"), 2))
        .orderBy("order_year", "order_month", "customer_segment", "value_segment")
    )


