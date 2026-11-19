from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, count, sum as spark_sum
)

# ==========================================
# 1. INITIALIZE SPARK
# ==========================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("RetailX-Bronze-to-Silver")
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://namenode:8020"
    )
    .config("spark.sql.defaultCatalog", "spark_catalog")
    .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE = "hdfs://namenode:8020/retailx-data-lake/bronze"
SILVER = "hdfs://namenode:8020/retailx-data-lake/silver"

# ==========================================
# 2. READ RAW ORDERS FROM BRONZE
# ==========================================

orders = (
    spark.read
    .option("header", True)
    .csv(f"{BRONZE}/orders/orders.csv")
)

print("\nRAW ORDERS")
orders.show(10, truncate=False)

print("Raw order count:", orders.count())

# ==========================================
# 3. DATA CLEANING
# ==========================================

# Convert data types and standardize strings
orders_clean = (
    orders
    .withColumn("order_id", col("order_id").cast("int"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("city", trim(col("city")))
    .withColumn("category", trim(col("category")))
    .withColumn("status", trim(col("status")))
    .withColumn("order_date", col("order_date").cast("date"))
)

# Remove duplicate order IDs
orders_clean = orders_clean.dropDuplicates(["order_id"])

# Separate invalid records
rejected_orders = orders_clean.filter(
    col("order_id").isNull() |
    col("amount").isNull() |
    (col("amount") <= 0) |
    col("customer_id").isNull() |
    (col("customer_id") == "")
)

# Keep only valid records
valid_orders = orders_clean.filter(
    col("order_id").isNotNull() &
    col("amount").isNotNull() &
    (col("amount") > 0) &
    col("customer_id").isNotNull() &
    (col("customer_id") != "")
)

print("\nCLEANED ORDERS")
valid_orders.show(10, truncate=False)

print("Valid orders:", valid_orders.count())
print("Rejected orders:", rejected_orders.count())

# ==========================================
# 4. WRITE ORDERS TO SILVER AS PARQUET
# ==========================================

valid_orders.write \
    .mode("overwrite") \
    .parquet(f"{SILVER}/orders")

rejected_orders.write \
    .mode("overwrite") \
    .parquet(f"{SILVER}/rejected_orders")

# ==========================================
# 5. READ AND CLEAN CUSTOMERS JSON
# ==========================================

customers = (
    spark.read
    .option("multiLine", True)
    .json(f"{BRONZE}/customers/customers.json")
)

customers_clean = (
    customers
    .select(
        col("customer_id"),
        trim(col("name")).alias("name"),
        lower(trim(col("email"))).alias("email"),
        col("location.city").alias("city"),
        col("location.country").alias("country"),
        col("membership")
    )
)

print("\nCLEANED CUSTOMERS")
customers_clean.show(10, truncate=False)

# Preserve customers with missing emails.
# Missing emails are represented as NULL.

customers_clean.write \
    .mode("overwrite") \
    .parquet(f"{SILVER}/customers")

# ==========================================
# 6. FINAL VERIFICATION
# ==========================================

print("\nSILVER LAYER VERIFICATION")

silver_orders = spark.read.parquet(
    f"{SILVER}/orders"
)

silver_customers = spark.read.parquet(
    f"{SILVER}/customers"
)

print("Silver orders:", silver_orders.count())
print("Silver customers:", silver_customers.count())

print("\nOrders schema:")
silver_orders.printSchema()

print("\nCustomer schema:")
silver_customers.printSchema()

print("\nTransformation completed successfully!")

spark.stop()
