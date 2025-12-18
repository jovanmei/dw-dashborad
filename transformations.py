"""
Core transformation logic for the e-commerce ETL pipeline.

This module contains reusable functions that can be unit-tested in
isolation from Spark job orchestration.
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    abs,
    avg,
    coalesce,
    col,
    count,
    current_date,
    datediff,
    lag,
    lit,
    max as _max,
    month,
    round as _round,
    sum as _sum,
    trim,
    upper,
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



def clean_and_enrich_orders(df_orders: DataFrame) -> DataFrame:
    """
    Clean dirty orders data and add enrichments.
    
    Handles:
    - Inconsistent date formats
    - Invalid status values
    - Negative/null amounts
    - Missing customer IDs
    """
    from data_cleaning_utils import standardize_dates, clean_order_status, clean_numeric_fields
    
    # Clean the data first
    cleaned = (df_orders
               .transform(lambda df: standardize_dates(df, "order_date"))
               .transform(clean_order_status)
               .transform(lambda df: clean_numeric_fields(df, "total_amount", min_value=0, max_value=50000))
               )
    
    # Add enrichments using cleaned data
    return (cleaned
            .withColumn("order_year", year(col("order_date_cleaned")))
            .withColumn("order_month", month(col("order_date_cleaned")))
            .withColumn("is_completed", when(col("status_cleaned") == "completed", 1).otherwise(0))
            .withColumn("is_valid_order", 
                       when((col("order_date_cleaned").isNotNull()) & 
                            (col("customer_id").isNotNull()) &
                            (col("total_amount_cleaned").isNotNull()) &
                            (col("total_amount_cleaned") > 0), 1).otherwise(0))
            )


def clean_and_enrich_customers(df_customers: DataFrame) -> DataFrame:
    """
    Clean dirty customer data and add enrichments.
    
    Handles:
    - Name standardization
    - Email validation
    - Date format inconsistencies
    - Segment standardization
    """
    from data_cleaning_utils import (
        clean_customer_names, clean_email_addresses, standardize_dates,
        clean_customer_segments, clean_boolean_fields, clean_phone_numbers,
        calculate_data_quality_score
    )
    
    # Apply all cleaning transformations
    cleaned = (df_customers
               .transform(clean_customer_names)
               .transform(clean_email_addresses)
               .transform(lambda df: standardize_dates(df, "join_date"))
               .transform(clean_customer_segments)
               .transform(lambda df: clean_boolean_fields(df, "marketing_opt_in"))
               .transform(lambda df: clean_phone_numbers(df, "phone"))
               )
    
    # Add data quality score
    required_fields = ["customer_id", "name_cleaned", "email_cleaned", "join_date_cleaned"]
    cleaned = cleaned.transform(lambda df: calculate_data_quality_score(df, required_fields))
    
    # Add customer tenure and age validation
    return (cleaned
            .withColumn("tenure_days", 
                       when(col("join_date_cleaned").isNotNull(),
                            datediff(current_date(), col("join_date_cleaned"))).otherwise(None))
            .withColumn("age_valid", 
                       when((col("age") >= 18) & (col("age") <= 120), col("age")).otherwise(None))
            )


def clean_and_enrich_order_items(df_order_items: DataFrame) -> DataFrame:
    """
    Clean dirty order items data and validate calculations.
    
    Handles:
    - Negative quantities/prices
    - Missing values
    - Calculation validation
    - Orphaned records
    """
    from data_cleaning_utils import clean_numeric_fields
    
    # Clean numeric fields
    cleaned = (df_order_items
               .transform(lambda df: clean_numeric_fields(df, "quantity", min_value=1, max_value=100))
               .transform(lambda df: clean_numeric_fields(df, "price", min_value=0, max_value=10000))
               .transform(lambda df: clean_numeric_fields(df, "discount_percent", min_value=0, max_value=100))
               )
    
    # Validate and recalculate line totals
    return (cleaned
            .withColumn("subtotal_calculated",
                       when((col("quantity_cleaned").isNotNull()) & (col("price_cleaned").isNotNull()),
                            col("quantity_cleaned") * col("price_cleaned")).otherwise(None))
            .withColumn("discount_amount_calculated",
                       when((col("subtotal_calculated").isNotNull()) & (col("discount_percent_cleaned").isNotNull()),
                            col("subtotal_calculated") * col("discount_percent_cleaned") / 100).otherwise(0))
            .withColumn("line_total_calculated",
                       when(col("subtotal_calculated").isNotNull(),
                            col("subtotal_calculated") - col("discount_amount_calculated") + 
                            coalesce(col("tax_amount"), lit(0))).otherwise(None))
            .withColumn("calculation_matches",
                       when((col("line_total").isNotNull()) & (col("line_total_calculated").isNotNull()),
                            abs(col("line_total") - col("line_total_calculated")) < 0.01).otherwise(False))
            )


def clean_and_enrich_products(df_products: DataFrame) -> DataFrame:
    """
    Clean dirty product data and add enrichments.
    
    Handles:
    - Name standardization
    - Category standardization
    - Price validation
    - Boolean field cleaning
    """
    from data_cleaning_utils import clean_numeric_fields, clean_boolean_fields, standardize_dates
    
    # Clean numeric and boolean fields
    cleaned = (df_products
               .transform(lambda df: clean_numeric_fields(df, "unit_price", min_value=0, max_value=100000))
               .transform(lambda df: clean_numeric_fields(df, "cost", min_value=0, max_value=100000))
               .transform(lambda df: clean_numeric_fields(df, "weight_kg", min_value=0, max_value=1000))
               .transform(lambda df: clean_boolean_fields(df, "is_active"))
               .transform(lambda df: standardize_dates(df, "created_date"))
               )
    
    # Clean product names and categories
    cleaned = (cleaned
               .withColumn("product_name_cleaned",
                          when((col("product_name").isNull()) | 
                               (trim(col("product_name")) == "") |
                               (upper(trim(col("product_name"))).isin(["UNKNOWN", "N/A", "TBD"])),
                               lit("Unknown Product")).otherwise(trim(col("product_name"))))
               .withColumn("category_cleaned",
                          when((col("category").isNull()) | 
                               (trim(col("category")) == "") |
                               (upper(trim(col("category"))).isin(["UNKNOWN", "N/A", "TBD"])),
                               lit("Other")).otherwise(
                                   when(upper(trim(col("category"))).contains("ELECTRONIC"), "Electronics")
                                   .when(upper(trim(col("category"))).contains("HOME"), "Home & Kitchen")
                                   .when(upper(trim(col("category"))).contains("KITCHEN"), "Home & Kitchen")
                                   .when(upper(trim(col("category"))).contains("SPORT"), "Sports & Outdoors")
                                   .when(upper(trim(col("category"))).contains("BOOK"), "Books")
                                   .otherwise(trim(col("category")))))
               )
    
    # Add business logic validations
    return (cleaned
            .withColumn("margin_percent",
                       when((col("unit_price_cleaned").isNotNull()) & 
                            (col("cost_cleaned").isNotNull()) &
                            (col("unit_price_cleaned") > 0),
                            _round(((col("unit_price_cleaned") - col("cost_cleaned")) / 
                                   col("unit_price_cleaned")) * 100, 2)).otherwise(None))
            .withColumn("price_cost_valid",
                       when((col("unit_price_cleaned").isNotNull()) & 
                            (col("cost_cleaned").isNotNull()),
                            col("cost_cleaned") <= col("unit_price_cleaned")).otherwise(None))
            )


def build_data_quality_summary(df_orders: DataFrame, df_customers: DataFrame, 
                              df_order_items: DataFrame, df_products: DataFrame) -> DataFrame:
    """
    Create a comprehensive data quality summary across all tables.
    
    Returns a DataFrame with quality metrics for each table.
    """
    from data_cleaning_utils import create_data_quality_report
    from pyspark.sql import SparkSession
    
    spark = SparkSession.getActiveSession()
    
    # Generate quality reports for each table
    orders_report = create_data_quality_report(df_orders, "orders")
    customers_report = create_data_quality_report(df_customers, "customers")
    order_items_report = create_data_quality_report(df_order_items, "order_items")
    products_report = create_data_quality_report(df_products, "products")
    
    # Convert to DataFrame format
    quality_data = [
        ("orders", orders_report["total_rows"], orders_report["total_columns"], 
         orders_report["duplicate_rows"], orders_report["completeness_score"]),
        ("customers", customers_report["total_rows"], customers_report["total_columns"],
         customers_report["duplicate_rows"], customers_report["completeness_score"]),
        ("order_items", order_items_report["total_rows"], order_items_report["total_columns"],
         order_items_report["duplicate_rows"], order_items_report["completeness_score"]),
        ("products", products_report["total_rows"], products_report["total_columns"],
         products_report["duplicate_rows"], products_report["completeness_score"])
    ]
    
    return spark.createDataFrame(quality_data, 
                                ["table_name", "total_rows", "total_columns", 
                                 "duplicate_rows", "completeness_score"])


def identify_referential_integrity_issues(df_orders: DataFrame, df_customers: DataFrame,
                                        df_order_items: DataFrame, df_products: DataFrame) -> dict:
    """
    Identify referential integrity issues between tables.
    
    Returns a dictionary with counts of orphaned records.
    """
    
    # Orders with missing customers
    orphaned_orders = (df_orders
                      .join(df_customers.select("customer_id"), "customer_id", "left_anti")
                      .filter(col("customer_id").isNotNull())
                      .count())
    
    # Order items with missing orders
    orphaned_order_items_orders = (df_order_items
                                  .join(df_orders.select("order_id"), "order_id", "left_anti")
                                  .filter(col("order_id").isNotNull())
                                  .count())
    
    # Order items with missing products
    orphaned_order_items_products = (df_order_items
                                    .join(df_products.select("product_id"), "product_id", "left_anti")
                                    .filter(col("product_id").isNotNull())
                                    .count())
    
    return {
        "orders_missing_customers": orphaned_orders,
        "order_items_missing_orders": orphaned_order_items_orders,
        "order_items_missing_products": orphaned_order_items_products
    }


def build_enhanced_fact_orders(df_orders_cleaned: DataFrame, df_customers_cleaned: DataFrame,
                              df_order_items_cleaned: DataFrame, df_products_cleaned: DataFrame) -> DataFrame:
    """
    Build an enhanced fact table using cleaned data with additional quality flags.
    
    This version handles dirty data gracefully and includes data quality indicators.
    """
    
    # Use cleaned columns for joins
    fact_orders = (df_orders_cleaned
                   .filter(col("is_valid_order") == 1)  # Only include valid orders
                   .join(df_customers_cleaned.select(
                       "customer_id", "name_cleaned", "segment_cleaned", "data_quality_score"
                   ), "customer_id", "left")
                   .join(df_order_items_cleaned.filter(
                       (col("quantity_cleaned").isNotNull()) & 
                       (col("price_cleaned").isNotNull())
                   ), "order_id", "left")
                   .join(df_products_cleaned.select(
                       "product_id", "product_name_cleaned", "category_cleaned", "is_active_cleaned"
                   ), "product_id", "left")
                   )
    
    return (fact_orders
            .select(
                col("order_id"),
                col("customer_id"),
                col("name_cleaned").alias("customer_name"),
                col("segment_cleaned").alias("customer_segment"),
                col("order_date_cleaned").alias("order_date"),
                col("order_year"),
                col("order_month"),
                col("status_cleaned").alias("status"),
                col("total_amount_cleaned").alias("total_amount"),
                col("product_id"),
                col("product_name_cleaned").alias("product_name"),
                col("category_cleaned").alias("product_category"),
                col("quantity_cleaned").alias("quantity"),
                col("price_cleaned").alias("unit_price"),
                col("line_total_calculated").alias("line_total"),
                col("is_completed"),
                col("data_quality_score").alias("customer_data_quality"),
                col("calculation_matches").alias("line_calc_valid"),
                col("is_active_cleaned").alias("product_active")
            )
            .filter(col("customer_name").isNotNull())  # Ensure we have customer data
            )


def detect_potential_fraud_orders(df_orders_cleaned: DataFrame, df_order_items_cleaned: DataFrame) -> DataFrame:
    """
    Identify potentially fraudulent orders based on suspicious patterns.
    
    Flags orders with:
    - Unusually high amounts
    - Multiple orders from same customer in short time
    - Suspicious quantity patterns
    """
    from pyspark.sql.window import Window
    
    # Calculate order statistics for fraud detection
    customer_window = Window.partitionBy("customer_id").orderBy("order_date_cleaned")
    
    orders_with_patterns = (df_orders_cleaned
                           .filter(col("is_valid_order") == 1)
                           .withColumn("prev_order_date", 
                                      lag("order_date_cleaned").over(customer_window))
                           .withColumn("days_since_last_order",
                                      when(col("prev_order_date").isNotNull(),
                                           datediff(col("order_date_cleaned"), col("prev_order_date")))
                                      .otherwise(None))
                           )
    
    # Join with order items to get quantity patterns
    orders_with_items = (orders_with_patterns
                        .join(df_order_items_cleaned
                              .filter(col("quantity_cleaned").isNotNull())
                              .groupBy("order_id")
                              .agg(_sum("quantity_cleaned").alias("total_quantity"),
                                   count("item_id").alias("item_count"),
                                   _max("quantity_cleaned").alias("max_item_quantity")),
                              "order_id", "left")
                        )
    
    # Flag suspicious orders with detailed scoring and fraud type classification
    scored_orders = (orders_with_items
                    .withColumn("amount_score", 
                               when(col("total_amount_cleaned") > 5000, 4)
                               .when(col("total_amount_cleaned") > 3000, 3)
                               .when(col("total_amount_cleaned") > 2000, 2)
                               .when(col("total_amount_cleaned") > 1000, 1)
                               .otherwise(0))
                    .withColumn("velocity_score",
                               when(col("days_since_last_order") < 1, 4)      # Same day
                               .when(col("days_since_last_order") < 3, 2)     # Within 3 days
                               .when(col("days_since_last_order") < 7, 1)     # Within a week
                               .otherwise(0))
                    .withColumn("quantity_score",
                               when(col("total_quantity") > 30, 3)
                               .when(col("total_quantity") > 20, 2)
                               .when(col("total_quantity") > 15, 1)
                               .otherwise(0))
                    .withColumn("item_score",
                               when(col("max_item_quantity") > 25, 3)
                               .when(col("max_item_quantity") > 15, 2)
                               .when(col("max_item_quantity") > 10, 1)
                               .otherwise(0))
                    )
    
    # Calculate total fraud score and classify fraud types
    return (scored_orders
            .withColumn("fraud_score", 
                       col("amount_score") + col("velocity_score") + 
                       col("quantity_score") + col("item_score"))
            .withColumn("fraud_type",
                       when((col("velocity_score") >= 2) & (col("amount_score") >= 2), "Velocity + High Value")
                       .when(col("velocity_score") >= 3, "Velocity Fraud")
                       .when(col("amount_score") >= 3, "High Value Fraud")
                       .when(col("quantity_score") >= 2, "Bulk Purchase Fraud")
                       .when(col("item_score") >= 2, "Suspicious Quantity")
                       .otherwise("Mixed Pattern"))
            .withColumn("risk_level",
                       when(col("fraud_score") >= 8, "Critical")
                       .when(col("fraud_score") >= 6, "High")
                       .when(col("fraud_score") >= 4, "Medium")
                       .otherwise("Low"))
            .withColumn("potential_fraud",
                       when(col("fraud_score") >= 3, True).otherwise(False))
            .filter(col("potential_fraud") == True)
            .select("order_id", "customer_id", "order_date_cleaned", "total_amount_cleaned",
                   "total_quantity", "max_item_quantity", "fraud_score", "fraud_type", 
                   "risk_level", "potential_fraud")
            )