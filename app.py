"""
Streamlit front-end for the E‑Commerce ETL project.

This app reads the Gold dashboard CSV export and provides interactive
visuals that you can show in a portfolio, demo, or interview.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st


GOLD_CSV_DIR = "exports/gold_customer_segment_monthly_csv"


# Support both newer Streamlit (`cache_data`) and older versions (`cache`)
_cache_decorator = getattr(st, "cache_data", getattr(st, "cache", None))


@_cache_decorator
def load_gold_dashboard() -> pd.DataFrame:
    """
    Load the Gold dashboard CSV (exported by main.py) into a pandas DataFrame.
    """

    if not os.path.isdir(GOLD_CSV_DIR):
        raise FileNotFoundError(
            f"Directory '{GOLD_CSV_DIR}' not found. "
            "Run 'python main.py' first to generate the CSV export."
        )

    # Spark writes a single part-*.csv file when we coalesce(1)
    csv_files = [
        f for f in os.listdir(GOLD_CSV_DIR) if f.endswith(".csv") and f.startswith("part-")
    ]
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in '{GOLD_CSV_DIR}'. "
            "Ensure write_gold_dashboard_csv is enabled in main.py."
        )

    path = os.path.join(GOLD_CSV_DIR, csv_files[0])
    return pd.read_csv(path)


def main() -> None:
    st.set_page_config(
        page_title="Revenue & Customer Segmentation",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("📊 DW ETL Dashboard")
    st.markdown(
        "Interactive dashboard powered by the **Gold** layer of the PySpark ETL pipeline.\n"
        "Data is aggregated by month, customer segment, and RFM value segment."
    )

    # Load data
    try:
        df = load_gold_dashboard()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.info(
            "Tip: In `main.py`, ensure `write_gold_dashboard_csv(...)` is enabled, "
            "run `python main.py`, then refresh this page."
        )
        return

    # Basic filters
    st.sidebar.header("Filters")

    years = sorted(df["order_year"].unique())
    selected_years = st.sidebar.multiselect(
        "Year", years, default=years
    )

    segments = sorted(df["customer_segment"].dropna().unique())
    selected_segments = st.sidebar.multiselect(
        "Customer Segment", segments, default=segments
    )

    value_segments = sorted(df["value_segment"].dropna().unique())
    selected_value_segments = st.sidebar.multiselect(
        "Value Segment (RFM)", value_segments, default=value_segments
    )

    filtered = df[
        df["order_year"].isin(selected_years)
        & df["customer_segment"].isin(selected_segments)
        & df["value_segment"].isin(selected_value_segments)
    ].copy()

    # KPI cards
    st.subheader("Key Performance Indicators")
    col1, col2, col3 = st.columns(3)

    total_revenue = filtered["revenue"].sum()
    total_orders = filtered["order_count"].sum()
    unique_customers = filtered["customer_count"].sum()

    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Orders", f"{int(total_orders):,}")
    col3.metric("Customer Appearances", f"{int(unique_customers):,}")

    # Revenue over time
    st.subheader("Revenue Trend by Month")

    filtered["year_month"] = (
        filtered["order_year"].astype(str)
        + "-"
        + filtered["order_month"].astype(str).str.zfill(2)
    )

    trend = (
        filtered.groupby("year_month", as_index=False)["revenue"]
        .sum()
        .sort_values("year_month")
    )
    st.line_chart(trend.set_index("year_month")["revenue"])

    # Revenue by customer segment and value segment
    left, right = st.columns(2)

    with left:
        st.subheader("Revenue by Customer Segment")
        seg = (
            filtered.groupby("customer_segment", as_index=False)["revenue"]
            .sum()
            .sort_values("revenue", ascending=False)
        )
        st.bar_chart(seg.set_index("customer_segment")["revenue"])

    with right:
        st.subheader("Revenue by Value Segment (RFM)")
        val = (
            filtered.groupby("value_segment", as_index=False)["revenue"]
            .sum()
            .sort_values("revenue", ascending=False)
        )
        st.bar_chart(val.set_index("value_segment")["revenue"])

    # Raw table
    st.subheader("Gold Layer Table (Sample Rows)")
    st.dataframe(filtered.sort_values(["order_year", "order_month"]).head(100))


if __name__ == "__main__":
    main()


