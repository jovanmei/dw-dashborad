"""
Data quality checks for the e-commerce ETL pipeline.

In a production setting these checks would feed into a monitoring /
alerting system (e.g. Great Expectations, Soda, or custom dashboards).
Here we keep them simple but explicit to highlight data engineering
practices.
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min as _min, sum as _sum, max as _max


def compute_null_counts(df: DataFrame) -> DataFrame:
    """
    Compute the number of nulls for each column in the given DataFrame.
    """

    return df.select(
        [_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )


def find_duplicate_keys(df: DataFrame, key_column: str) -> DataFrame:
    """
    Identify duplicate records based on a primary-key-like column.
    """

    return df.groupBy(key_column).count().filter(col("count") > 1)


def compute_date_range(df: DataFrame, date_column: str) -> Dict[str, str]:
    """
    Compute min/max date for a given date column.
    """

    range_row = df.select(
        _min(col(date_column)).alias("earliest"), _max(col(date_column)).alias("latest")
    ).collect()[0]

    return {"earliest": str(range_row["earliest"]), "latest": str(range_row["latest"])}


