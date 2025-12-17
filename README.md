# E-Commerce Data Warehouse ETL Pipeline (PySpark)

A production-ready, modular PySpark ETL pipeline implementing a **medallion architecture** (Bronze/Silver/Gold) for an e-commerce data warehouse. This project demonstrates enterprise data engineering practices including data quality checks, dimensional modeling, customer segmentation, and anomaly detection.

## 🎯 Business Context

**Problem**: An online retail company needs to consolidate data from multiple sources (transactional orders, CRM customers, product catalog) for business intelligence and analytics.

**Solution**: A scalable ETL pipeline that:
- **Ingests** raw data from CSV, JSON, and Parquet sources
- **Validates** data quality (nulls, duplicates, date ranges)
- **Transforms** data into dimensional models (fact tables, customer metrics)
- **Enriches** with business logic (RFM segmentation, anomaly detection)
- **Exposes** dashboard-ready aggregates in a Gold layer for BI tools

**Business Value**:
- Enables data-driven decision making through consolidated analytics
- Identifies high-value customers for targeted marketing campaigns
- Provides early warning system for revenue anomalies
- Supports self-service BI with pre-aggregated dashboard tables

---

## 📁 Project Structure

```
etl-project/
├── main.py                    # Pipeline orchestrator (entry point)
├── config.py                  # Configuration (paths, Spark settings)
├── spark_session.py           # SparkSession factory
├── data_generation.py         # Synthetic data generation (fallback)
├── quality_checks.py          # Data quality validation functions
├── transformations.py         # Core business logic transformations
├── analytics.py               # Reporting and visualization helpers
├── io_utils.py                # File I/O (read/write) utilities
├── init_raw_data.py           # Utility script to generate sample input files
├── requirements.txt           # Python dependencies
├── data/raw/                  # Raw input files (CSV/JSON/Parquet)
├── lake/                      # Medallion architecture outputs
│   ├── bronze/                # Raw ingested data
│   ├── silver/                # Cleaned, modeled data
│   └── gold/                  # Dashboard-ready aggregates
└── exports/                   # CSV exports for Excel/BI tools
```

---

## 🔧 Module Descriptions

### `main.py` - Pipeline Orchestrator
**Purpose**: Entry point that coordinates the entire ETL workflow.

**Key Functions**:
- `run_pipeline()`: Main orchestration function that executes all pipeline steps sequentially
- `run_quality_checks()`: Wrapper function that executes data quality validation

**Pipeline Steps**:
1. **Load Source Data**: Attempts to read from `data/raw/`, falls back to synthetic data if files missing
2. **Data Quality Checks**: Validates nulls, duplicates, date ranges
3. **Transformations**: Enriches orders, builds fact tables, computes customer metrics
4. **Analytics**: Generates business intelligence reports (revenue, segments, anomalies)
5. **Persist Medallion Layers**: Writes Bronze (raw), Silver (cleaned), Gold (aggregated) tables

---

### `config.py` - Configuration Management
**Purpose**: Centralized configuration for Spark settings and data paths.

**Classes**:
- `SparkConfig`: Spark application settings (app name, shuffle partitions)
- `Paths`: Data lake paths for raw inputs and medallion layers (Bronze/Silver/Gold)

**Key Paths**:
- Raw inputs: `data/raw/{orders.csv, customers.json, order_items.csv, products.parquet}`
- Bronze layer: `lake/bronze/{orders, customers, order_items, products}`
- Silver layer: `lake/silver/{orders_enriched, fact_orders, customer_metrics}`
- Gold layer: `lake/gold/customer_segment_monthly`

---

### `spark_session.py` - Spark Session Factory
**Purpose**: Creates and configures SparkSession with project-specific settings.

**Functions**:
- `create_spark_session()`: Returns a configured SparkSession with app name and shuffle partitions from `SPARK_CONFIG`

**Why Separate**: Enables reuse across jobs and easier unit testing of transformations without Spark dependencies.

---

### `data_generation.py` - Synthetic Data Generation
**Purpose**: Creates realistic in-memory datasets when raw files are unavailable.

**Functions**:
- `create_sample_dataframes(spark)`: Returns tuple of (orders, customers, order_items, products) DataFrames

**Data Schemas**:
- **Orders**: `order_id`, `customer_id`, `order_date`, `status`, `total_amount`
- **Customers**: `customer_id`, `name`, `email`, `join_date`, `segment` (Premium/Regular)
- **Order Items**: `item_id`, `order_id`, `product_id`, `quantity`, `price`
- **Products**: `product_id`, `product_name`, `category`, `unit_price`

**Use Case**: Enables pipeline to run end-to-end without external dependencies, useful for demos and testing.

---

### `quality_checks.py` - Data Quality Validation
**Purpose**: Reusable functions for data quality checks that could feed into monitoring/alerting systems.

**Functions**:
- `compute_null_counts(df)`: Returns DataFrame with null counts per column
- `find_duplicate_keys(df, key_column)`: Identifies duplicate records based on a primary key column
- `compute_date_range(df, date_column)`: Returns dict with `earliest` and `latest` dates

**Production Note**: In production, these would integrate with tools like Great Expectations, Soda, or custom dashboards.

---

### `transformations.py` - Core Business Logic
**Purpose**: Reusable transformation functions that can be unit-tested independently.

#### Data Enrichment Functions:
- `enrich_orders(df_orders)`: Adds derived columns (`order_year`, `order_month`, `is_completed` flag)

#### Dimensional Modeling Functions:
- `build_fact_orders(df_orders_enriched, df_customers, df_order_items, df_products)`: Creates denormalized fact table joining all dimensions (orders, customers, products, order items)
- `build_customer_metrics(df_orders_enriched, df_customers)`: Aggregates customer-level KPIs (total_orders, total_revenue, avg_order_value, tenure_days)

#### Analytics Functions:
- `build_monthly_revenue(df_orders_enriched)`: Computes monthly revenue trend for completed orders
- `build_segment_analysis(fact_orders)`: Revenue and order volume by customer segment (Premium/Regular)
- `build_status_distribution(df_orders)`: Distribution of order statuses (completed, cancelled, pending)
- `build_category_performance(fact_orders)`: Category-level revenue and units sold

#### Advanced Analytics Functions:
- `build_customer_rfm(fact_orders)`: **RFM Segmentation** - Calculates Recency (days since last order), Frequency (order count), Monetary (total revenue) and assigns value segments (`high_value`, `medium_value`, `low_value`)
  - **Business Value**: Enables marketing/CRM teams to target high-value customers and identify at-risk customers for retention campaigns
- `detect_monthly_revenue_anomalies(monthly_revenue, drop_threshold=0.2)`: **Anomaly Detection** - Flags months where revenue drops by >20% vs previous month using window functions
  - **Business Value**: Early warning system for demand drops, operational issues, or market changes
- `build_customer_segment_monthly_dashboard(fact_orders, customer_rfm)`: **Gold Layer Table** - Creates dashboard-ready aggregate table with one row per (year, month, customer_segment, value_segment) containing revenue, order_count, and customer_count
  - **Business Value**: Pre-aggregated table optimized for BI tools (Power BI, Tableau) to consume directly

---

### `analytics.py` - Reporting & Visualization
**Purpose**: Helper functions to format and display business insights in a user-friendly way.

**Functions**:
- `print_overall_revenue_metrics(df_orders_enriched)`: Displays total revenue, completed orders, average order value
- `print_top_customers(df_customer_metrics, top_n=5)`: Shows top N customers by revenue with order metrics
- `print_dataframe_with_title(title, df)`: Generic helper to print DataFrames with formatted titles
- `print_customer_rfm(rfm)`: Displays RFM segments with focus on high-value customers for targeted campaigns
- `print_revenue_anomalies(anomalies)`: Shows months with significant revenue drops or confirms no anomalies detected

**Note**: In production, these would write to BI dashboards or export reports instead of printing to console.

---

### `io_utils.py` - Input/Output Utilities
**Purpose**: Handles all file I/O operations, abstracting data lake/warehouse connectors.

#### Read Functions (with fallback):
- `read_orders(spark)`: Reads orders from CSV, returns `None` if file missing
- `read_customers(spark)`: Reads customers from JSON (one object per line)
- `read_order_items(spark)`: Reads order items from CSV
- `read_products(spark)`: Reads products from Parquet

**Helper**: `_path_exists(spark, path)`: Checks if a path exists using Hadoop FileSystem API

#### Write Functions - Medallion Architecture:
- `write_bronze_tables(df_orders, df_customers, df_order_items, df_products)`: Persists raw ingested data to Bronze layer (Parquet format)
- `write_silver_tables(df_orders_enriched, fact_orders, df_customer_metrics)`: Writes cleaned and modeled tables to Silver layer (partitioned by year/month where applicable)
- `write_gold_dashboard(df_dashboard)`: Persists Gold layer dashboard table (Parquet)
- `write_gold_dashboard_csv(df_dashboard, output_dir)`: **Export to CSV** - Exports Gold dashboard as single CSV file for Excel/BI tools (coalesces to 1 partition for single file output)

**Production Note**: In production, these would connect to S3, Delta Lake, Snowflake, BigQuery, etc.

---

### `init_raw_data.py` - Data Setup Utility
**Purpose**: Standalone script to generate sample input files in `data/raw/` directory.

**Usage**:
```bash
python init_raw_data.py
```

**What it does**: Uses `create_sample_dataframes()` to generate synthetic data and writes them in the exact formats expected by the pipeline:
- `data/raw/orders.csv` (CSV with header)
- `data/raw/order_items.csv` (CSV with header)
- `data/raw/customers.json` (JSON, one object per line)
- `data/raw/products.parquet` (Parquet)

**Use Case**: Run once to set up demo data, then the pipeline will use file-based inputs instead of synthetic data.

---

## 🚀 Getting Started

### Prerequisites
- **Python 3.8+**
- **Java 8+** (required for PySpark)
- Set `JAVA_HOME` environment variable

### Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd etl-project
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **(Optional) Generate sample input files**:
```bash
python init_raw_data.py
```

This creates `data/raw/` with sample CSV, JSON, and Parquet files. If you skip this step, the pipeline will automatically generate synthetic data in-memory.

### Running the Pipeline

**Execute the main ETL pipeline**:
```bash
python main.py
```

**What happens**:
1. Loads data from `data/raw/` (or generates synthetic data if files missing)
2. Runs data quality checks
3. Applies transformations and builds dimensional models
4. Generates business intelligence reports (printed to console)
5. Writes medallion layers to `lake/bronze/`, `lake/silver/`, `lake/gold/`
6. Exports Gold dashboard to CSV in `exports/gold_customer_segment_monthly_csv/`

**Output Locations**:
- **Bronze**: `lake/bronze/{orders, customers, order_items, products}/`
- **Silver**: `lake/silver/{orders_enriched, fact_orders, customer_metrics}/`
- **Gold**: `lake/gold/customer_segment_monthly/`
- **CSV Export**: `exports/gold_customer_segment_monthly_csv/part-*.csv` (open in Excel)

---

## 📊 Pipeline Architecture

### Medallion Architecture (Bronze/Silver/Gold)

```
┌─────────────────┐
│  Raw Sources    │  CSV, JSON, Parquet
│  (data/raw/)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   BRONZE LAYER  │  Raw ingested data (as-is)
│  (lake/bronze/) │  - orders, customers, order_items, products
└────────┬────────┘
         │
         ▼ Data Quality Checks
┌─────────────────┐
│   SILVER LAYER  │  Cleaned & modeled data
│  (lake/silver/) │  - orders_enriched (with derived columns)
│                 │  - fact_orders (denormalized fact table)
│                 │  - customer_metrics (aggregated KPIs)
└────────┬────────┘
         │
         ▼ Business Logic Transformations
┌─────────────────┐
│    GOLD LAYER   │  Dashboard-ready aggregates
│   (lake/gold/)  │  - customer_segment_monthly (pre-aggregated)
└────────┬────────┘
         │
         ▼ CSV Export
┌─────────────────┐
│   BI Tools      │  Excel, Power BI, Tableau
│   (exports/)    │
└─────────────────┘
```

---

## 📈 Key Features & Business Value

### 1. **Data Quality Assurance**
- Null detection, duplicate identification, date range validation
- **Value**: Ensures data reliability before downstream analytics

### 2. **Dimensional Modeling**
- Star schema with `fact_orders` fact table and dimension tables
- **Value**: Optimized for analytical queries and BI tool consumption

### 3. **Customer Segmentation (RFM)**
- Recency, Frequency, Monetary analysis with value segments
- **Value**: Enables targeted marketing campaigns and customer retention strategies

### 4. **Anomaly Detection**
- Monthly revenue drop alerts (>20% threshold)
- **Value**: Early warning system for business issues requiring investigation

### 5. **Medallion Architecture**
- Bronze (raw) → Silver (cleaned) → Gold (aggregated) layers
- **Value**: Scalable data lake pattern supporting incremental processing and data lineage

### 6. **Dashboard-Ready Outputs**
- Pre-aggregated Gold tables optimized for BI tools
- CSV exports for Excel-based analysis
- **Value**: Self-service analytics without requiring SQL expertise

---

## 🛠️ Customization

### Adjusting Configuration
Edit `config.py` to modify:
- Spark settings (app name, shuffle partitions)
- Data paths (raw inputs, medallion layers)
- Output locations

### Adding New Transformations
1. Add transformation function to `transformations.py`
2. Import and call in `main.py` within the appropriate pipeline step
3. Optionally add write function to `io_utils.py` if persisting to a new table

### Modifying Data Quality Checks
Edit `quality_checks.py` to add new validation functions (e.g., value range checks, referential integrity).

---

## 📝 Resume Bullets

**For Data Engineering Roles**:
- "Designed and implemented a production-ready PySpark ETL pipeline using medallion architecture (Bronze/Silver/Gold) for an e-commerce data warehouse, processing multi-source data (CSV, JSON, Parquet) with automated data quality checks and dimensional modeling"
- "Built customer RFM segmentation and revenue anomaly detection modules enabling marketing teams to identify high-value customers and detect business issues early, resulting in actionable business intelligence"
- "Architected scalable data lake structure with partitioned Parquet outputs and CSV exports, supporting both programmatic analytics and self-service BI tools (Power BI, Tableau)"

---

## 🔍 Technical Highlights

- **Modular Design**: Separation of concerns (config, I/O, transformations, analytics)
- **Type Hints**: Full Python type annotations for better IDE support and maintainability
- **Error Handling**: Graceful fallback from file-based to synthetic data
- **Scalability**: Spark-based distributed processing ready for large datasets
- **Production Patterns**: Medallion architecture, data quality checks, partitioned outputs
- **Business Focus**: Clear business value demonstrated through RFM segmentation and anomaly detection

---

## 📄 License

This project is provided as-is for educational and portfolio purposes.
