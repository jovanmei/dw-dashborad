"""
Utility script to materialise raw input files under `data/raw/`.

This reuses the synthetic datasets from `data_generation.py` and writes
them out in the exact formats expected by the ETL pipeline:

- `data/raw/orders.csv`        (CSV)
- `data/raw/order_items.csv`   (CSV)
- `data/raw/customers.json`    (JSON, one object per line)
- `data/raw/products.parquet`  (Parquet)
"""

from __future__ import annotations

from config import PATHS
from data_generation import create_sample_dataframes
from spark_session import create_spark_session


def create_raw_files() -> None:
    spark = create_spark_session()

    df_orders, df_customers, df_order_items, df_products = create_sample_dataframes(
        spark
    )

    print("Writing raw orders CSV ->", PATHS.raw_orders)
    (
        df_orders.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(PATHS.raw_orders)
    )

    print("Writing raw order_items CSV ->", PATHS.raw_order_items)
    (
        df_order_items.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(PATHS.raw_order_items)
    )

    print("Writing raw customers JSON ->", PATHS.raw_customers)
    df_customers.coalesce(1).write.mode("overwrite").json(PATHS.raw_customers)

    print("Writing raw products Parquet ->", PATHS.raw_products)
    df_products.coalesce(1).write.mode("overwrite").parquet(PATHS.raw_products)

    print("All raw files created under data/raw/.")


if __name__ == "__main__":
    create_raw_files()


