"""
Spark session factory.

Keeping Spark session creation in a separate module makes it easier to
reuse this code across jobs and to unit test transformation logic
without having to duplicate Spark configuration.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from config import SPARK_CONFIG


def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession for the ETL pipeline.
    """

    return (
        SparkSession.builder.appName(SPARK_CONFIG.app_name)
        .config("spark.sql.shuffle.partitions", str(SPARK_CONFIG.shuffle_partitions))
        .getOrCreate()
    )


