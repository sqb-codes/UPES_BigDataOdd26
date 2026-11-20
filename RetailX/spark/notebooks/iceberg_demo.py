
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("RetailX-Iceberg-Demo")

    # The image's spark-defaults.conf makes "demo" (http://rest:8181) the
    # default catalog; that host doesn't exist here, so use ours instead.
    .config("spark.sql.defaultCatalog", "retailx")

    # Connect to the persistent REST catalog
    .config(
        "spark.sql.catalog.retailx",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.retailx.type",
        "rest"
    )
    .config(
        "spark.sql.catalog.retailx.uri",
        "http://retailx-iceberg-rest:8181"
    )
    .config(
        "spark.sql.catalog.retailx.warehouse",
        "s3://warehouse/"
    )

    # Iceberg S3 FileIO configuration
    .config(
        "spark.sql.catalog.retailx.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        "spark.sql.catalog.retailx.s3.endpoint",
        "http://seaweedfs:8333"
    )
    .config(
        "spark.sql.catalog.retailx.s3.path-style-access",
        "true"
    )
    .config(
        "spark.sql.catalog.retailx.s3.access-key-id",
        "admin"
    )
    .config(
        "spark.sql.catalog.retailx.s3.secret-access-key",
        "retailx_password"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Create namespace
spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS retailx.demo
""")

# Create an Iceberg table using format version 2
spark.sql("""
    CREATE TABLE IF NOT EXISTS retailx.demo.sales_demo (
        order_id BIGINT,
        city STRING,
        category STRING,
        amount DOUBLE
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

# Insert sample data
spark.sql("""
    INSERT INTO retailx.demo.sales_demo VALUES
    (1001, 'Delhi', 'Electronics', 25000.0),
    (1002, 'Mumbai', 'Fashion', 5000.0),
    (1003, 'Pune', 'Electronics', 18000.0)
""")

# Display records
print("ICEBERG TABLE DATA")
spark.sql("""
    SELECT * FROM retailx.demo.sales_demo
""").show()

# Display table details
print("ICEBERG TABLE DETAILS")
spark.sql("""
    DESCRIBE EXTENDED retailx.demo.sales_demo
""").show(50, truncate=False)

print("ICEBERG TABLE CREATED SUCCESSFULLY")

spark.stop()
