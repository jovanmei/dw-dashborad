"""
E‑Commerce Data Warehouse ETL Pipeline
=====================================

This module orchestrates an end‑to‑end PySpark ETL job that:

- synthesises source data for orders, customers, order items and products
- runs basic data quality checks
- builds a dimensional model-style `fact_orders` table
- computes customer and product level KPIs
- writes curated datasets to disk (parquet)

The pipeline is intentionally compact but demonstrates the structure of a
real-world data engineering project: separate configuration, Spark
session management, transformations, quality checks and IO.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from analytics import (
    print_customer_rfm,
    print_dataframe_with_title,
    print_overall_revenue_metrics,
    print_revenue_anomalies,
    print_top_customers,
)
from data_generation import create_sample_dataframes
from io_utils import (
    read_customers,
    read_order_items,
    read_orders,
    read_products,
    write_bronze_tables,
    write_customer_metrics,
    write_fact_orders,
    write_gold_dashboard,
    write_gold_dashboard_csv,
    write_silver_tables,
)
from quality_checks import compute_date_range, compute_null_counts, find_duplicate_keys
from spark_session import create_spark_session
from transformations import (
    build_category_performance,
    build_customer_metrics,
    build_customer_rfm,
    build_customer_segment_monthly_dashboard,
    build_fact_orders,
    build_monthly_revenue,
    build_segment_analysis,
    build_status_distribution,
    detect_monthly_revenue_anomalies,
    enrich_orders,
)


def run_quality_checks(df_orders: DataFrame) -> None:
    print("\n[STEP 2] Performing data quality checks...")

    nulls = compute_null_counts(df_orders)
    print("\nNull counts in orders:")
    nulls.show()

    duplicates = find_duplicate_keys(df_orders, "order_id")
    print(f"Duplicate orders: {duplicates.count()}")

    date_range = compute_date_range(df_orders, "order_date")
    print("\nOrder date range:")
    print(date_range)


def run_pipeline() -> None:
    spark = create_spark_session()

    print("=" * 80)
    print("E-COMMERCE DATA WAREHOUSE ETL PIPELINE")
    print("=" * 80)

    # ------------------------------------------------------------------
    # Step 1 – Source data
    # ------------------------------------------------------------------
    print("\n[STEP 1] Loading source datasets...")

    # Prefer file-based inputs when they exist (more realistic for a DE project),
    # otherwise fall back to synthetic data generation.
    df_orders = read_orders(spark)
    df_customers = read_customers(spark)
    df_order_items = read_order_items(spark)
    df_products = read_products(spark)

    if any(df is None for df in (df_orders, df_customers, df_order_items, df_products)):
        print("Some input files are missing; falling back to synthetic in-memory data.")
        df_orders, df_customers, df_order_items, df_products = create_sample_dataframes(
            spark
        )

    print(f"✓ Orders rows: {df_orders.count()}")
    print(f"✓ Customers rows: {df_customers.count()}")
    print(f"✓ Products rows: {df_products.count()}")

    # ------------------------------------------------------------------
    # Step 2 – Data quality (on raw / Bronze)
    # ------------------------------------------------------------------
    run_quality_checks(df_orders)

    # ------------------------------------------------------------------
    # Step 3 – Transformations
    # ------------------------------------------------------------------
    print("\n[STEP 3] Applying transformations...")
    df_orders_enriched = enrich_orders(df_orders)
    df_customer_metrics = build_customer_metrics(df_orders_enriched, df_customers)
    fact_orders = build_fact_orders(
        df_orders_enriched, df_customers, df_order_items, df_products
    )
    print(f"✓ Fact table created with {fact_orders.count()} records")

    # ------------------------------------------------------------------
    # Step 4 – Analytics / reporting
    # ------------------------------------------------------------------
    print("\n[STEP 5] Generating business insights...")
    print("\n" + "=" * 80)
    print("BUSINESS INTELLIGENCE REPORT")
    print("=" * 80)

    print_overall_revenue_metrics(df_orders_enriched)
    print_top_customers(df_customer_metrics, top_n=5)

    segment_analysis = build_segment_analysis(fact_orders)
    monthly_revenue = build_monthly_revenue(df_orders_enriched)
    status_distribution = build_status_distribution(df_orders)
    category_performance = build_category_performance(fact_orders)
    customer_rfm = build_customer_rfm(fact_orders)
    revenue_anomalies = detect_monthly_revenue_anomalies(monthly_revenue)
    dashboard_table = build_customer_segment_monthly_dashboard(fact_orders, customer_rfm)

    print_dataframe_with_title("3. REVENUE BY CUSTOMER SEGMENT", segment_analysis)
    print_dataframe_with_title("4. MONTHLY REVENUE TREND", monthly_revenue)
    print_dataframe_with_title("5. ORDER STATUS DISTRIBUTION", status_distribution)
    print_dataframe_with_title("6. PRODUCT CATEGORY PERFORMANCE", category_performance)
    print_customer_rfm(customer_rfm)
    print_revenue_anomalies(revenue_anomalies)

    # ------------------------------------------------------------------
    # Step 6 – Persist medallion layers (Bronze / Silver / Gold)
    # ------------------------------------------------------------------
    print("\n[STEP 6] Saving medallion layers (Bronze / Silver / Gold)...")
    # Commented out to keep the project side‑effect free by default.
    # Uncomment when you want to actually materialise parquet outputs.
    #
    # Bronze: raw ingested
    write_bronze_tables(df_orders, df_customers, df_order_items, df_products)
    #
    # Silver: cleaned & modeled
    write_silver_tables(df_orders_enriched, fact_orders, df_customer_metrics)
    #
    # Gold: dashboard-ready aggregate
    write_gold_dashboard(dashboard_table)
    #
    # Optional: Export Gold dashboard to CSV for Excel
    write_gold_dashboard_csv(dashboard_table, "exports/gold_customer_segment_monthly_csv")

    print("✓ Bronze, Silver, Gold layers ready for export")
    print("✓ Dashboard table ready for BI tools")
    print("✓ Analytics reports generated")

    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 80)

    # In long-running applications you would usually stop Spark explicitly
    # spark.stop()


if __name__ == "__main__":
    run_pipeline()


