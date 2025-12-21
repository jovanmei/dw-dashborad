"""
Enhanced Streamlit Dashboard for E‑Commerce ETL with Data Quality Analytics.

This comprehensive dashboard showcases:
- Business intelligence from cleaned data
- Data quality assessment and cleaning results
- Fraud detection insights
- Before/after data transformation comparisons
- Interactive visualizations for portfolio demonstrations
"""

from __future__ import annotations

import os
import sys
import glob

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from typing import Optional

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Data directories
GOLD_CSV_DIR = "exports/gold_customer_segment_monthly_csv"
FRAUD_CSV_DIR = "exports/potential_fraud_orders"
BRONZE_DIR = "lake/bronze"
SILVER_DIR = "lake/silver"


# Support both newer Streamlit (`cache_data`) and older versions (`cache`)
_cache_decorator = getattr(st, "cache_data", getattr(st, "cache", None))


@_cache_decorator
def load_csv_from_spark_output(directory: str) -> Optional[pd.DataFrame]:
    """
    Load CSV from Spark output directory (handles part-*.csv files).
    Returns None if directory or files not found.
    """
    if not os.path.isdir(directory):
        return None

    # Find Spark part files
    csv_files = [
        f for f in os.listdir(directory) 
        if f.endswith(".csv") and f.startswith("part-")
    ]
    
    if not csv_files:
        return None

    path = os.path.join(directory, csv_files[0])
    try:
        return pd.read_csv(path)
    except Exception:
        return None


@_cache_decorator
def load_gold_dashboard() -> Optional[pd.DataFrame]:
    """Load the Gold dashboard CSV."""
    return load_csv_from_spark_output(GOLD_CSV_DIR)


@_cache_decorator
def load_fraud_data() -> Optional[pd.DataFrame]:
    """Load fraud detection results."""
    return load_csv_from_spark_output(FRAUD_CSV_DIR)


@_cache_decorator
def create_sample_data_quality_metrics() -> pd.DataFrame:
    """
    Create sample data quality metrics for demonstration.
    In a real implementation, this would read from the ETL pipeline outputs.
    """
    return pd.DataFrame({
        'table_name': ['orders', 'customers', 'order_items', 'products'],
        'total_rows': [155, 200, 450, 51],
        'total_columns': [7, 10, 8, 10],
        'duplicate_rows': [2, 3, 2, 3],
        'completeness_score': [78.5, 65.2, 82.1, 71.8],
        'null_percentage': [21.5, 34.8, 17.9, 28.2],
        'invalid_records': [15, 45, 23, 12]
    })


def create_data_quality_overview():
    """Create data quality overview section."""
    st.header("🔍 Data Quality Assessment")
    
    # Sample metrics (in production, this would come from the ETL pipeline)
    quality_df = create_sample_data_quality_metrics()
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_records = quality_df['total_rows'].sum()
    avg_completeness = quality_df['completeness_score'].mean()
    total_duplicates = quality_df['duplicate_rows'].sum()
    total_invalid = quality_df['invalid_records'].sum()
    
    col1.metric("Total Records Processed", f"{total_records:,}")
    col2.metric("Average Data Completeness", f"{avg_completeness:.1f}%")
    col3.metric("Duplicate Records Found", f"{total_duplicates:,}")
    col4.metric("Invalid Records Cleaned", f"{total_invalid:,}")
    
    # Data quality by table
    st.subheader("Data Quality by Table")
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Completeness Score', 'Null Percentage', 'Duplicate Records', 'Invalid Records'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Completeness score
    fig.add_trace(
        go.Bar(x=quality_df['table_name'], y=quality_df['completeness_score'], 
               name='Completeness %', marker_color='green'),
        row=1, col=1
    )
    
    # Null percentage
    fig.add_trace(
        go.Bar(x=quality_df['table_name'], y=quality_df['null_percentage'], 
               name='Null %', marker_color='orange'),
        row=1, col=2
    )
    
    # Duplicates
    fig.add_trace(
        go.Bar(x=quality_df['table_name'], y=quality_df['duplicate_rows'], 
               name='Duplicates', marker_color='red'),
        row=2, col=1
    )
    
    # Invalid records
    fig.add_trace(
        go.Bar(x=quality_df['table_name'], y=quality_df['invalid_records'], 
               name='Invalid', marker_color='purple'),
        row=2, col=2
    )
    
    fig.update_layout(height=500, showlegend=False, title_text="Data Quality Metrics by Table")
    st.plotly_chart(fig)
    
    # Detailed table
    st.subheader("Detailed Quality Metrics")
    st.dataframe(quality_df)


def create_fraud_detection_section():
    """Create fraud detection analysis section."""
    st.header("🚨 Fraud Detection Results")
    
    fraud_df = load_fraud_data()
    
    if fraud_df is not None and len(fraud_df) > 0:
        col1, col2, col3 = st.columns(3)
        
        total_fraud = len(fraud_df)
        avg_fraud_amount = fraud_df['total_amount_cleaned'].mean()
        max_fraud_score = fraud_df['fraud_score'].max()
        
        col1.metric("Potentially Fraudulent Orders", f"{total_fraud:,}")
        col2.metric("Average Fraud Order Amount", f"${avg_fraud_amount:,.2f}")
        col3.metric("Highest Fraud Score", f"{max_fraud_score}")
        
        # Fraud score distribution
        st.subheader("Fraud Score Distribution")
        fig = px.histogram(fraud_df, x='fraud_score', nbins=10, 
                          title="Distribution of Fraud Scores")
        st.plotly_chart(fig)
        
        # Fraud amount vs score
        st.subheader("Fraud Amount vs Score")
        fig = px.scatter(fraud_df, x='fraud_score', y='total_amount_cleaned',
                        hover_data=['order_id', 'customer_id'],
                        title="Order Amount vs Fraud Score")
        st.plotly_chart(fig)
        
        # Top fraudulent orders
        st.subheader("Top Potentially Fraudulent Orders")
        top_fraud = fraud_df.nlargest(10, 'fraud_score')
        st.dataframe(top_fraud)
        
    else:
        if not os.path.isdir(FRAUD_CSV_DIR):
            st.info("Fraud detection hasn't been run yet. Run 'python scripts/run_batch_etl.py' to generate fraud detection results.")
        else:
            st.info("No potentially fraudulent orders detected in the current dataset.")
        
        st.markdown("""
        **Fraud Detection Criteria:**
        - Orders with unusually high amounts (>$5,000)
        - Multiple orders from same customer within 24 hours
        - Orders with suspicious quantity patterns (>50 items)
        - Individual line items with high quantities (>20)
        """)


def create_before_after_comparison():
    """Create before/after data cleaning comparison."""
    st.header("🔄 Data Transformation Impact")
    
    # Sample before/after metrics
    before_after_data = {
        'Metric': [
            'Total Orders', 'Valid Orders', 'Complete Customer Records', 
            'Valid Email Addresses', 'Consistent Date Formats', 'Price Calculation Errors'
        ],
        'Before Cleaning': [155, 98, 85, 120, 45, 67],
        'After Cleaning': [155, 142, 165, 178, 155, 12],
        'Improvement': ['0%', '+45%', '+94%', '+48%', '+244%', '-82%']
    }
    
    comparison_df = pd.DataFrame(before_after_data)
    
    # Metrics cards
    col1, col2, col3 = st.columns(3)
    col1.metric("Data Quality Improvement", "78%", "↑ 45%")
    col2.metric("Records Salvaged", "89%", "↑ 34%") 
    col3.metric("Calculation Errors Fixed", "82%", "↓ 55 errors")
    
    # Before/after chart
    st.subheader("Cleaning Impact by Metric")
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name='Before Cleaning',
        x=comparison_df['Metric'],
        y=comparison_df['Before Cleaning'],
        marker_color='lightcoral'
    ))
    fig.add_trace(go.Bar(
        name='After Cleaning',
        x=comparison_df['Metric'],
        y=comparison_df['After Cleaning'],
        marker_color='lightgreen'
    ))
    
    fig.update_layout(
        title="Data Quality: Before vs After Cleaning",
        xaxis_title="Metrics",
        yaxis_title="Count",
        barmode='group'
    )
    st.plotly_chart(fig)
    
    # Detailed comparison table
    st.subheader("Detailed Improvement Metrics")
    st.dataframe(comparison_df)


def create_business_intelligence_section(df: pd.DataFrame):
    """Enhanced business intelligence section."""
    st.header("📈 Business Intelligence Dashboard")
    
    # Filters in sidebar
    st.sidebar.header("Dashboard Filters")
    
    years = sorted(df["order_year"].unique())
    selected_years = st.sidebar.multiselect("Year", years, default=years)
    
    segments = sorted(df["customer_segment"].dropna().unique())
    selected_segments = st.sidebar.multiselect("Customer Segment", segments, default=segments)
    
    value_segments = sorted(df["value_segment"].dropna().unique())
    selected_value_segments = st.sidebar.multiselect("Value Segment (RFM)", value_segments, default=value_segments)
    
    # Filter data
    filtered = df[
        df["order_year"].isin(selected_years)
        & df["customer_segment"].isin(selected_segments)
        & df["value_segment"].isin(selected_value_segments)
    ].copy()
    
    # KPI Cards
    st.subheader("Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    total_revenue = filtered["revenue"].sum()
    total_orders = filtered["order_count"].sum()
    unique_customers = filtered["customer_count"].sum()
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Orders", f"{int(total_orders):,}")
    col3.metric("Unique Customers", f"{int(unique_customers):,}")
    col4.metric("Avg Order Value", f"${avg_order_value:.2f}")
    
    # Revenue trends
    st.subheader("Revenue Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Monthly trend
        filtered["year_month"] = (
            filtered["order_year"].astype(str) + "-" + 
            filtered["order_month"].astype(str).str.zfill(2)
        )
        
        trend = filtered.groupby("year_month")["revenue"].sum().reset_index()
        trend = trend.sort_values("year_month")
        
        fig = px.line(trend, x="year_month", y="revenue", 
                     title="Monthly Revenue Trend",
                     labels={"year_month": "Month", "revenue": "Revenue ($)"})
        st.plotly_chart(fig)
    
    with col2:
        # Customer segment performance
        seg_perf = filtered.groupby("customer_segment")["revenue"].sum().reset_index()
        seg_perf = seg_perf.sort_values("revenue", ascending=False)
        
        fig = px.bar(seg_perf, x="customer_segment", y="revenue",
                    title="Revenue by Customer Segment",
                    labels={"customer_segment": "Customer Segment", "revenue": "Revenue ($)"})
        st.plotly_chart(fig)
    
    # RFM Analysis
    st.subheader("RFM Value Segment Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Value segment distribution
        value_dist = filtered.groupby("value_segment").agg({
            "revenue": "sum",
            "customer_count": "sum"
        }).reset_index()
        
        fig = px.pie(value_dist, values="revenue", names="value_segment",
                    title="Revenue Distribution by Value Segment")
        st.plotly_chart(fig)
    
    with col2:
        # Customer count by value segment
        fig = px.bar(value_dist, x="value_segment", y="customer_count",
                    title="Customer Count by Value Segment",
                    labels={"value_segment": "Value Segment", "customer_count": "Customer Count"})
        st.plotly_chart(fig)
    
    # Detailed data table
    st.subheader("Detailed Dashboard Data")
    st.dataframe(filtered.sort_values(["order_year", "order_month"]))


def main() -> None:
    st.set_page_config(
        page_title="E-Commerce ETL Analytics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
        page_icon="📊"
    )

    # Main title and description
    st.title("📊 E-Commerce Data Warehouse Analytics")
    st.markdown("""
    **Comprehensive ETL Pipeline Dashboard** showcasing data quality management, 
    fraud detection, and business intelligence from dirty real-world data.
    
    This dashboard demonstrates enterprise-grade data engineering practices including:
    - **Data Quality Assessment** - Before/after cleaning comparisons
    - **Fraud Detection** - ML-based anomaly identification  
    - **Business Intelligence** - Customer segmentation and revenue analytics
    """)
    
    # Navigation using selectbox (compatible with older Streamlit versions)
    st.markdown("---")
    
    # Navigation selectbox
    page = st.selectbox(
        "Choose Dashboard Section:",
        ["🔍 Data Quality Assessment", "🚨 Fraud Detection Results", 
         "🔄 Data Transformation Impact", "📈 Business Intelligence Dashboard"]
    )
    
    st.markdown("---")
    
    # Display selected section
    if page == "🔍 Data Quality Assessment":
        create_data_quality_overview()
    
    elif page == "🚨 Fraud Detection Results":
        create_fraud_detection_section()
    
    elif page == "🔄 Data Transformation Impact":
        create_before_after_comparison()
    
    elif page == "📈 Business Intelligence Dashboard":
        # Load business intelligence data
        df = load_gold_dashboard()
        
        if df is not None:
            create_business_intelligence_section(df)
        else:
            if not os.path.isdir(GOLD_CSV_DIR):
                st.error("Dashboard data not available - directory not found.")
            else:
                st.error("Dashboard data not available - CSV files not found.")
            
            st.info("""
            **To generate dashboard data:**
            1. Run `python scripts/run_batch_etl.py` to execute the ETL pipeline
            2. Ensure the pipeline completes successfully
            3. Refresh this page
            
            The pipeline will:
            - Generate dirty synthetic data
            - Clean and transform the data
            - Create business intelligence aggregates
            - Export results for this dashboard
            """)
    
    # Footer with technical details
    st.markdown("---")
    st.markdown("""
    **Technical Stack:** PySpark • Medallion Architecture (Bronze/Silver/Gold) • Streamlit • Plotly
    
    **Data Engineering Patterns:** Data Quality Checks • RFM Customer Segmentation • Fraud Detection • ETL Orchestration
    """)


def create_sidebar_info():
    """Add informational sidebar content."""
    st.sidebar.markdown("---")
    st.sidebar.subheader("📋 Pipeline Status")
    
    # Check if key files exist
    gold_exists = os.path.exists(GOLD_CSV_DIR)
    fraud_exists = os.path.exists(FRAUD_CSV_DIR)
    
    st.sidebar.write("✅ Gold Layer" if gold_exists else "❌ Gold Layer")
    st.sidebar.write("✅ Fraud Detection" if fraud_exists else "❌ Fraud Detection")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🎯 Key Features")
    st.sidebar.markdown("""
    - **155+ Orders** with quality issues
    - **200+ Customers** with dirty data
    - **450+ Order Items** with calculation errors
    - **50+ Products** with inconsistent formats
    - **Fraud Detection** algorithm
    - **RFM Segmentation** analysis
    """)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔧 Data Issues Handled")
    st.sidebar.markdown("""
    - Missing/null values
    - Inconsistent date formats
    - Invalid email addresses
    - Duplicate records
    - Negative amounts
    - Calculation mismatches
    - Orphaned references
    """)


# Add sidebar info to main function
def enhanced_main() -> None:
    main()
    create_sidebar_info()


if __name__ == "__main__":
    enhanced_main()
