"""
Input/output helpers for the ETL pipeline.

Right now this module only contains simple write helpers, but having it
separate makes it obvious where you would plug in data lake / warehouse
connectors (e.g. S3, Delta Lake, Snowflake, BigQuery).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from config import PATHS


def _path_exists(spark: SparkSession, path: str) -> bool:
    """
    Small helper to check whether a path exists in the current environment.
    """

    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        )
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        # In local testing environments we fail open and let the caller fall
        # back to synthetic data.
        return False


def read_orders(spark: SparkSession) -> DataFrame | None:
    """
    Try to read orders from CSV; return None if the file is missing.

    Expected schema (headered CSV):
      order_id,customer_id,order_date,status,total_amount
    """

    if not _path_exists(spark, PATHS.raw_orders):
        return None

    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(PATHS.raw_orders)
    )


def read_customers(spark: SparkSession) -> DataFrame | None:
    """
    Try to read customers from JSON; return None if the file is missing.
    """

    if not _path_exists(spark, PATHS.raw_customers):
        return None

    return spark.read.json(PATHS.raw_customers)


def read_order_items(spark: SparkSession) -> DataFrame | None:
    """
    Try to read order items from CSV; return None if the file is missing.
    """

    if not _path_exists(spark, PATHS.raw_order_items):
        return None

    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(PATHS.raw_order_items)
    )


def read_products(spark: SparkSession) -> DataFrame | None:
    """
    Try to read products from Parquet; return None if the file is missing.
    """

    if not _path_exists(spark, PATHS.raw_products):
        return None

    return spark.read.parquet(PATHS.raw_products)


def write_customer_metrics(df: DataFrame, mode: str = "overwrite") -> None:
    """
    Persist customer metrics to parquet.
    """

    df.write.mode(mode).parquet(PATHS.customer_metrics)


def write_fact_orders(df: DataFrame, mode: str = "overwrite") -> None:
    """
    Persist fact_orders to parquet partitioned by year/month.
    """

    df.write.mode(mode).partitionBy("order_year", "order_month").parquet(
        PATHS.fact_orders
    )


def write_bronze_tables(
    df_orders: DataFrame,
    df_customers: DataFrame,
    df_order_items: DataFrame,
    df_products: DataFrame,
    mode: str = "overwrite",
) -> None:
    """
    Persist raw-but-ingested datasets into the Bronze layer.
    """

    df_orders.write.mode(mode).parquet(f"{PATHS.bronze_base}/orders")
    df_customers.write.mode(mode).parquet(f"{PATHS.bronze_base}/customers")
    df_order_items.write.mode(mode).parquet(f"{PATHS.bronze_base}/order_items")
    df_products.write.mode(mode).parquet(f"{PATHS.bronze_base}/products")


def write_silver_tables(
    df_orders_enriched: DataFrame,
    fact_orders: DataFrame,
    df_customer_metrics: DataFrame,
    mode: str = "overwrite",
) -> None:
    """
    Persist cleaned and modeled tables into the Silver layer.
    """

    df_orders_enriched.write.mode(mode).partitionBy("order_year", "order_month").parquet(
        f"{PATHS.silver_base}/orders_enriched"
    )
    fact_orders.write.mode(mode).partitionBy("order_year", "order_month").parquet(
        f"{PATHS.silver_base}/fact_orders"
    )
    df_customer_metrics.write.mode(mode).parquet(
        f"{PATHS.silver_base}/customer_metrics"
    )


def write_gold_dashboard(df_dashboard: DataFrame, mode: str = "overwrite") -> None:
    """
    Persist a denormalised, dashboard-ready table into the Gold layer.
    """

    df_dashboard.write.mode(mode).parquet(f"{PATHS.gold_base}/customer_segment_monthly")


def write_gold_dashboard_csv(df_dashboard: DataFrame, output_dir: str) -> None:
    """
    Export the Gold dashboard table as a single CSV file that can be
    opened directly in Excel.
    """

    (
        df_dashboard.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_dir)
    )

