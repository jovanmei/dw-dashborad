"""
Analytics and reporting helpers.

These functions use the derived DataFrames produced during the ETL
pipeline to generate business-friendly views. In this simple project we
just print them, but they could be written to BI tables or exported.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, sum as _sum


def print_overall_revenue_metrics(df_orders_enriched: DataFrame) -> None:
    print("\n1. OVERALL REVENUE METRICS")
    print("-" * 80)

    metrics = (
        df_orders_enriched.filter(col("status") == "completed")
        .agg(
            _sum("total_amount").alias("total_revenue"),
            count("order_id").alias("completed_orders"),
            avg("total_amount").alias("avg_order_value"),
        )
    )
    metrics.show()


def print_top_customers(df_customer_metrics: DataFrame, top_n: int = 5) -> None:
    print(f"\n2. TOP {top_n} CUSTOMERS BY REVENUE")
    print("-" * 80)

    df_customer_metrics.orderBy(col("total_revenue").desc()).limit(top_n).select(
        "customer_id", "total_orders", "total_revenue", "avg_order_value", "segment"
    ).show()


def print_dataframe_with_title(title: str, df: DataFrame) -> None:
    """
    Convenience helper to keep console output consistent.
    """

    print(f"\n{title}")
    print("-" * 80)
    df.show()


def print_customer_rfm(rfm: DataFrame) -> None:
    """
    Print RFM segments with a focus on high-value customers.
    """

    print("\n7. CUSTOMER VALUE SEGMENTS (RFM)")
    print("-" * 80)
    rfm.orderBy(col("monetary").desc()).show()

    print("\nHigh-value customers (for targeted retention campaigns):")
    rfm.filter(col("value_segment") == "high_value").orderBy(
        col("monetary").desc()
    ).show()


def print_revenue_anomalies(anomalies: DataFrame) -> None:
    """
    Show months where revenue has dropped significantly vs previous month.
    """

    if anomalies.rdd.isEmpty():
        print("\nNo significant monthly revenue drops detected.")
        return

    print("\n8. REVENUE ANOMALY ALERTS")
    print("-" * 80)
    anomalies.show()


