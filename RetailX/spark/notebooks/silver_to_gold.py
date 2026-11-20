
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, countDistinct, avg,
    round, desc
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("RetailX-Silver-to-Gold")
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://namenode:8020"
    )
    .config("spark.sql.defaultCatalog", "spark_catalog")
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181")
    .getOrCreate()
)

# --------------------------------------------------
# 2. Define HDFS paths
# --------------------------------------------------

SILVER = "hdfs://namenode:8020/retailx-data-lake/silver"
GOLD = "hdfs://namenode:8020/retailx-data-lake/gold"

# --------------------------------------------------
# 3. Read Silver datasets
# --------------------------------------------------

orders = spark.read.parquet(f"{SILVER}/orders")
customers = spark.read.parquet(f"{SILVER}/customers")

print("Silver Orders:")
orders.show(5, truncate=False)

print("Silver Customers:")
customers.show(5, truncate=False)

# --------------------------------------------------
# 4. City-wise sales analytics
# --------------------------------------------------

city_sales = (
    orders
    .groupBy("city")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("amount"), 2).alias("total_revenue"),
        round(avg("amount"), 2).alias("average_order_value")
    )
    .orderBy(desc("total_revenue"))
)

city_sales.write.mode("overwrite").parquet(
    f"{GOLD}/city_sales"
)

print("CITY-WISE SALES")
city_sales.show(truncate=False)

# --------------------------------------------------
# 5. Category-wise sales analytics
# --------------------------------------------------

category_sales = (
    orders
    .groupBy("category")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("amount"), 2).alias("total_revenue"),
        round(avg("amount"), 2).alias("average_order_value")
    )
    .orderBy(desc("total_revenue"))
)

category_sales.write.mode("overwrite").parquet(
    f"{GOLD}/category_sales"
)

print("CATEGORY-WISE SALES")
category_sales.show(truncate=False)

# --------------------------------------------------
# 6. Customer-wise analytics
# --------------------------------------------------

customer_sales = (
    orders
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("amount"), 2).alias("total_spent"),
        round(avg("amount"), 2).alias("average_order_value")
    )
    .join(customers, on="customer_id", how="left")
    .select(
        "customer_id",
        "name",
        "city",
        "membership",
        "total_orders",
        "total_spent",
        "average_order_value"
    )
    .orderBy(desc("total_spent"))
)

customer_sales.write.mode("overwrite").parquet(
    f"{GOLD}/customer_sales"
)

print("TOP CUSTOMERS")
customer_sales.show(10, truncate=False)

# --------------------------------------------------
# 7. Verify Gold datasets
# --------------------------------------------------

print("Gold Layer Record Counts")

print("City Sales:", city_sales.count())
print("Category Sales:", category_sales.count())
print("Customer Sales:", customer_sales.count())

print("Gold layer generation completed successfully!")

spark.stop()
