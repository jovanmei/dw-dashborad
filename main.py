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
from pyspark.sql.functions import col

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
    # New dirty data handling functions
    clean_and_enrich_orders,
    clean_and_enrich_customers,
    clean_and_enrich_order_items,
    clean_and_enrich_products,
    build_data_quality_summary,
    identify_referential_integrity_issues,
    build_enhanced_fact_orders,
    detect_potential_fraud_orders,
)


def run_quality_checks(df_orders: DataFrame, df_customers: DataFrame, 
                      df_order_items: DataFrame, df_products: DataFrame) -> None:
    print("\n[STEP 2] Performing comprehensive data quality checks...")

    # Original quality checks for orders
    nulls = compute_null_counts(df_orders)
    print("\nNull counts in orders:")
    nulls.show(truncate=False)

    duplicates = find_duplicate_keys(df_orders, "order_id")
    print(f"Duplicate orders: {duplicates.count()}")
    if duplicates.count() > 0:
        print("Sample duplicate orders:")
        duplicates.show(5, truncate=False)

    date_range = compute_date_range(df_orders, "order_date")
    print("\nOrder date range:")
    print(date_range)
    
    # Enhanced quality checks across all tables
    print("\n" + "="*60)
    print("COMPREHENSIVE DATA QUALITY ASSESSMENT")
    print("="*60)
    
    # Data quality summary
    quality_summary = build_data_quality_summary(df_orders, df_customers, df_order_items, df_products)
    print("\nData Quality Summary:")
    quality_summary.show(truncate=False)
    
    # Referential integrity issues
    integrity_issues = identify_referential_integrity_issues(df_orders, df_customers, df_order_items, df_products)
    print("\nReferential Integrity Issues:")
    for issue, count in integrity_issues.items():
        print(f"  {issue}: {count} records")
    
    # Sample of dirty data issues
    print("\nSample Data Quality Issues Found:")
    
    # Show orders with invalid dates
    invalid_dates = df_orders.filter(
        col("order_date").isNull() | 
        col("order_date").rlike(r"2024-13-|2025-")
    )
    if invalid_dates.count() > 0:
        print(f"\nInvalid order dates ({invalid_dates.count()} records):")
        invalid_dates.select("order_id", "order_date", "status").show(5, truncate=False)
    
    # Show customers with invalid emails
    invalid_emails = df_customers.filter(
        col("email").isNull() | 
        (~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
    )
    if invalid_emails.count() > 0:
        print(f"\nInvalid customer emails ({invalid_emails.count()} records):")
        invalid_emails.select("customer_id", "name", "email").show(5, truncate=False)
    
    # Show negative amounts
    negative_amounts = df_orders.filter(col("total_amount") < 0)
    if negative_amounts.count() > 0:
        print(f"\nNegative order amounts ({negative_amounts.count()} records):")
        negative_amounts.select("order_id", "customer_id", "total_amount", "status").show(5, truncate=False)


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
    run_quality_checks(df_orders, df_customers, df_order_items, df_products)

    # ------------------------------------------------------------------
    # Step 3 – Data Cleaning & Transformations
    # ------------------------------------------------------------------
    print("\n[STEP 3] Cleaning dirty data and applying transformations...")
    
    # Clean all datasets first
    print("  → Cleaning orders data...")
    df_orders_cleaned = clean_and_enrich_orders(df_orders)
    
    print("  → Cleaning customers data...")
    df_customers_cleaned = clean_and_enrich_customers(df_customers)
    
    print("  → Cleaning order items data...")
    df_order_items_cleaned = clean_and_enrich_order_items(df_order_items)
    
    print("  → Cleaning products data...")
    df_products_cleaned = clean_and_enrich_products(df_products)
    
    # Show cleaning results
    valid_orders = df_orders_cleaned.filter(col("is_valid_order") == 1).count()
    total_orders = df_orders_cleaned.count()
    print(f"✓ Orders: {valid_orders}/{total_orders} valid after cleaning ({(valid_orders/total_orders)*100:.1f}%)")
    
    # Build enhanced fact table with cleaned data
    print("  → Building enhanced fact table...")
    fact_orders = build_enhanced_fact_orders(
        df_orders_cleaned, df_customers_cleaned, df_order_items_cleaned, df_products_cleaned
    )
    print(f"✓ Enhanced fact table created with {fact_orders.count()} records")
    
    # Also create traditional metrics for comparison
    df_customer_metrics = build_customer_metrics(df_orders_cleaned.filter(col("is_valid_order") == 1), 
                                                df_customers_cleaned)

    # ------------------------------------------------------------------
    # Step 4 – Advanced Analytics & Fraud Detection
    # ------------------------------------------------------------------
    print("\n[STEP 4] Detecting fraud and generating business insights...")
    
    # Fraud detection on cleaned data
    print("  → Running fraud detection...")
    potential_fraud = detect_potential_fraud_orders(df_orders_cleaned, df_order_items_cleaned)
    fraud_count = potential_fraud.count()
    print(f"✓ Identified {fraud_count} potentially fraudulent orders")
    
    if fraud_count > 0:
        print("\nSample potentially fraudulent orders:")
        potential_fraud.show(5, truncate=False)
    
    # ------------------------------------------------------------------
    # Step 5 – Business Intelligence Reporting
    # ------------------------------------------------------------------
    print("\n[STEP 5] Generating comprehensive business insights...")
    print("\n" + "=" * 80)
    print("ENHANCED BUSINESS INTELLIGENCE REPORT")
    print("(Based on Cleaned Data)")
    print("=" * 80)

    # Use cleaned data for all analytics
    valid_orders_df = df_orders_cleaned.filter(col("is_valid_order") == 1)
    print_overall_revenue_metrics(valid_orders_df)
    print_top_customers(df_customer_metrics, top_n=5)

    segment_analysis = build_segment_analysis(fact_orders)
    monthly_revenue = build_monthly_revenue(valid_orders_df)
    status_distribution = build_status_distribution(df_orders_cleaned)
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
    
    # Additional insights from cleaned data
    print_dataframe_with_title("7. DATA QUALITY SUMMARY", 
                              build_data_quality_summary(df_orders, df_customers, df_order_items, df_products))
    
    if fraud_count > 0:
        print_dataframe_with_title("8. FRAUD DETECTION RESULTS", potential_fraud)

    # ------------------------------------------------------------------
    # Step 6 – Persist medallion layers (Bronze / Silver / Gold)
    # ------------------------------------------------------------------
    print("\n[STEP 6] Saving enhanced medallion layers...")
    
    # Bronze: raw ingested (dirty data as-is)
    print("  → Writing Bronze layer (raw dirty data)...")
    write_bronze_tables(df_orders, df_customers, df_order_items, df_products)
    
    # Silver: cleaned & modeled data
    print("  → Writing Silver layer (cleaned data)...")
    write_silver_tables(valid_orders_df, fact_orders, df_customer_metrics)
    
    # Gold: dashboard-ready aggregate
    print("  → Writing Gold layer (dashboard tables)...")
    write_gold_dashboard(dashboard_table)
    
    # Export Gold dashboard to CSV for Excel/BI tools
    print("  → Exporting Gold layer to CSV...")
    write_gold_dashboard_csv(dashboard_table, "exports/gold_customer_segment_monthly_csv")
    
    # Save fraud detection results
    if fraud_count > 0:
        print("  → Saving fraud detection results...")
        potential_fraud.coalesce(1).write.mode("overwrite").option("header", "true").csv("exports/potential_fraud_orders")

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


