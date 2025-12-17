"""
Simple test to isolate the data generation issue.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

def test_order_items():
    spark = SparkSession.builder.appName("TestDataGen").getOrCreate()
    
    # Simple test data
    order_items_data = [
        (1, 1, 201, 2, 75.00, 10.0, 6.00, 81.00),
        (2, 2, 202, 1, 320.50, 0.0, 25.64, 346.14),
        (3, 3, 203, 1, 89.99, 5.0, 7.20, 92.69),
    ]
    
    order_items_schema = StructType([
        StructField("item_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("line_total", DoubleType(), True),
    ])
    
    df = spark.createDataFrame(order_items_data, order_items_schema)
    df.show()
    print("Test successful!")
    
    spark.stop()

if __name__ == "__main__":
    test_order_items()