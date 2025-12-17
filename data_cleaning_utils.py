"""
Data cleaning utilities for handling dirty, real-world data quality issues.

This module provides functions to clean and standardize messy data commonly
found in production systems, including:
- Date format standardization
- Email validation and cleaning
- Name standardization
- Null value handling
- Data type conversions
- Duplicate detection and removal
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_replace, trim, lower, upper, coalesce, lit,
    to_date, to_timestamp, isnan, isnull, length, regexp_extract,
    split, concat_ws, round as spark_round, sum as spark_sum
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType


def clean_customer_names(df: DataFrame) -> DataFrame:
    """
    Clean and standardize customer names.
    
    - Removes extra whitespace
    - Standardizes case (Title Case)
    - Handles null/empty values
    - Removes invalid entries like "Unknown", "N/A", "TBD"
    """
    return df.withColumn(
        "name_cleaned",
        when(
            (col("name").isNull()) | 
            (trim(col("name")) == "") |
            (upper(trim(col("name"))).isin(["UNKNOWN", "N/A", "TBD", "TEST USER"])),
            lit(None)
        ).otherwise(
            # Clean and standardize name format
            regexp_replace(
                regexp_replace(trim(col("name")), r"\s+", " "),  # Multiple spaces to single
                r"^(Mr\.|Ms\.|Mrs\.|Dr\.)\s*", ""  # Remove titles
            )
        )
    )


def clean_email_addresses(df: DataFrame) -> DataFrame:
    """
    Clean and validate email addresses.
    
    - Converts to lowercase
    - Validates email format
    - Marks invalid emails as null
    """
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    
    return df.withColumn(
        "email_cleaned",
        when(
            (col("email").isNull()) |
            (trim(col("email")) == "") |
            (upper(trim(col("email"))).isin(["N/A", "TBD", "UNKNOWN", "NOTANEMAIL"])) |
            (~col("email").rlike(email_pattern)),
            lit(None)
        ).otherwise(
            lower(trim(col("email")))
        )
    )


def standardize_dates(df: DataFrame, date_column: str, output_column: str = None) -> DataFrame:
    """
    Standardize date formats to YYYY-MM-DD.
    
    Handles multiple input formats:
    - YYYY-MM-DD (ISO)
    - MM/DD/YYYY (US)
    - DD-MM-YYYY (European)
    - YYYYMMDD (Compact)
    - Month DD, YYYY (Long format)
    """
    if output_column is None:
        output_column = f"{date_column}_cleaned"
    
    return df.withColumn(
        output_column,
        coalesce(
            # Try ISO format first (YYYY-MM-DD)
            to_date(col(date_column), "yyyy-MM-dd"),
            # Try US format (MM/DD/YYYY)
            to_date(col(date_column), "MM/dd/yyyy"),
            # Try European format (DD-MM-YYYY)
            to_date(col(date_column), "dd-MM-yyyy"),
            # Try compact format (YYYYMMDD)
            to_date(col(date_column), "yyyyMMdd"),
            # Try long format (Month DD, YYYY)
            to_date(col(date_column), "MMMM dd, yyyy"),
            # If all fail, return null
            lit(None)
        )
    )


def clean_order_status(df: DataFrame) -> DataFrame:
    """
    Standardize order status values.
    
    - Converts to lowercase
    - Maps variations to standard values
    - Handles typos and inconsistencies
    """
    return df.withColumn(
        "status_cleaned",
        when(
            (col("status").isNull()) | (trim(col("status")) == ""),
            lit("unknown")
        ).when(
            upper(trim(col("status"))).isin(["COMPLETED", "COMPLETE"]),
            lit("completed")
        ).when(
            upper(trim(col("status"))).isin(["CANCELLED", "CANCELED"]),
            lit("cancelled")
        ).when(
            upper(trim(col("status"))).isin(["PENDING", "PROCESSING"]),
            lit("pending")
        ).when(
            upper(trim(col("status"))).isin(["SHIPPED", "DELIVERED"]),
            lit("shipped")
        ).when(
            upper(trim(col("status"))).isin(["REFUNDED", "RETURNED"]),
            lit("refunded")
        ).when(
            upper(trim(col("status"))).isin(["FAILED", "ERROR"]),
            lit("failed")
        ).when(
            upper(trim(col("status"))).isin(["UNKNWON"]),  # Handle typo
            lit("unknown")
        ).otherwise(
            lower(trim(col("status")))
        )
    )


def clean_customer_segments(df: DataFrame) -> DataFrame:
    """
    Standardize customer segment values.
    """
    return df.withColumn(
        "segment_cleaned",
        when(
            (col("segment").isNull()) | 
            (trim(col("segment")) == "") |
            (upper(trim(col("segment"))).isin(["UNKNOWN", "N/A", "TBD"])),
            lit("Unknown")
        ).when(
            upper(trim(col("segment"))).isin(["PREMIUM", "VIP", "GOLD"]),
            lit("Premium")
        ).when(
            upper(trim(col("segment"))).isin(["REGULAR", "STANDARD", "BASIC", "SILVER"]),
            lit("Regular")
        ).when(
            upper(trim(col("segment"))).isin(["BRONZE", "NEW"]),
            lit("Bronze")
        ).otherwise(
            # Keep original value but clean it - just trim and title case
            trim(col("segment"))
        )
    )


def clean_boolean_fields(df: DataFrame, column_name: str, output_column: str = None) -> DataFrame:
    """
    Standardize boolean fields that may have inconsistent formats.
    
    Handles: true/false, True/False, 1/0, Y/N, Yes/No, etc.
    """
    if output_column is None:
        output_column = f"{column_name}_cleaned"
    
    return df.withColumn(
        output_column,
        when(
            upper(trim(col(column_name).cast("string"))).isin(
                ["TRUE", "T", "YES", "Y", "1", "ACTIVE"]
            ),
            lit(True)
        ).when(
            upper(trim(col(column_name).cast("string"))).isin(
                ["FALSE", "F", "NO", "N", "0", "INACTIVE"]
            ),
            lit(False)
        ).otherwise(
            lit(None)
        ).cast(BooleanType())
    )


def clean_numeric_fields(df: DataFrame, column_name: str, 
                        min_value: Optional[float] = None,
                        max_value: Optional[float] = None,
                        output_column: str = None) -> DataFrame:
    """
    Clean numeric fields by removing invalid values.
    
    - Sets negative values to null (unless min_value allows them)
    - Sets values outside reasonable ranges to null
    - Handles null and zero values appropriately
    """
    if output_column is None:
        output_column = f"{column_name}_cleaned"
    
    cleaned_col = col(column_name)
    
    # Apply minimum value constraint
    if min_value is not None:
        cleaned_col = when(col(column_name) < min_value, lit(None)).otherwise(cleaned_col)
    
    # Apply maximum value constraint
    if max_value is not None:
        cleaned_col = when(col(column_name) > max_value, lit(None)).otherwise(cleaned_col)
    
    return df.withColumn(output_column, cleaned_col)


def detect_and_flag_duplicates(df: DataFrame, key_columns: list, flag_column: str = "is_duplicate") -> DataFrame:
    """
    Detect duplicate records based on key columns and add a flag.
    
    The first occurrence is marked as False, subsequent duplicates as True.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    # Create window partitioned by key columns, ordered by all columns for consistency
    window = Window.partitionBy(*key_columns).orderBy(*df.columns)
    
    return df.withColumn(
        flag_column,
        when(row_number().over(window) > 1, lit(True)).otherwise(lit(False))
    )


def calculate_data_quality_score(df: DataFrame, 
                               required_columns: list,
                               score_column: str = "data_quality_score") -> DataFrame:
    """
    Calculate a data quality score (0-100) based on completeness of required fields.
    
    Score = (number of non-null required fields / total required fields) * 100
    """
    # Count non-null values in required columns
    non_null_count = sum([
        when(col(column).isNotNull() & (col(column) != ""), 1).otherwise(0)
        for column in required_columns
    ])
    
    total_required = len(required_columns)
    
    return df.withColumn(
        score_column,
        spark_round((non_null_count / total_required) * 100, 2)
    )


def clean_phone_numbers(df: DataFrame, phone_column: str = "phone", 
                       output_column: str = None) -> DataFrame:
    """
    Clean and standardize phone numbers to a consistent format.
    
    Converts various formats to: +1-XXX-XXX-XXXX
    """
    if output_column is None:
        output_column = f"{phone_column}_cleaned"
    
    return df.withColumn(
        output_column,
        when(
            (col(phone_column).isNull()) |
            (trim(col(phone_column)) == "") |
            (upper(trim(col(phone_column))).isin(["N/A", "UNKNOWN", "TBD"])) |
            (col(phone_column).rlike(r"^123-456-7890$")),  # Fake number
            lit(None)
        ).otherwise(
            # Extract digits only and reformat
            concat_ws("-",
                lit("+1"),
                regexp_extract(regexp_replace(col(phone_column), r"[^\d]", ""), r"(\d{3})", 1),
                regexp_extract(regexp_replace(col(phone_column), r"[^\d]", ""), r"\d{3}(\d{3})", 1),
                regexp_extract(regexp_replace(col(phone_column), r"[^\d]", ""), r"\d{6}(\d{4})", 1)
            )
        )
    )


def identify_outliers_iqr(df: DataFrame, column_name: str, 
                         multiplier: float = 1.5,
                         flag_column: str = None) -> DataFrame:
    """
    Identify outliers using the IQR (Interquartile Range) method.
    
    Values outside Q1 - multiplier*IQR or Q3 + multiplier*IQR are flagged as outliers.
    """
    if flag_column is None:
        flag_column = f"{column_name}_is_outlier"
    
    # Calculate quartiles
    quantiles = df.select(col(column_name)).approxQuantile(column_name, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    
    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr
    
    return df.withColumn(
        flag_column,
        when(
            (col(column_name) < lower_bound) | (col(column_name) > upper_bound),
            lit(True)
        ).otherwise(lit(False))
    )


def create_data_quality_report(df: DataFrame, table_name: str) -> dict:
    """
    Generate a comprehensive data quality report for a DataFrame.
    
    Returns a dictionary with various data quality metrics.
    """
    total_rows = df.count()
    
    # Calculate null counts for each column
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | (col(column) == "")).count()
        null_counts[column] = {
            "null_count": null_count,
            "null_percentage": round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
        }
    
    # Calculate duplicate count if there's an ID column
    duplicate_count = 0
    id_columns = [col for col in df.columns if 'id' in col.lower()]
    if id_columns:
        primary_id = id_columns[0]  # Use first ID column found
        unique_count = df.select(primary_id).distinct().count()
        duplicate_count = total_rows - unique_count
    
    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "duplicate_rows": duplicate_count,
        "null_analysis": null_counts,
        "completeness_score": round(
            sum([100 - metrics["null_percentage"] for metrics in null_counts.values()]) / len(null_counts), 2
        ) if null_counts else 0
    }