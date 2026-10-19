from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


# Create Spark session
spark = (
    SparkSession.builder
    .appName("SalesAnalytics")
    .master("local[*]")
    .getOrCreate()
)

# Read cleaned Parquet data
input_path = "data/processed/clean_sales.parquet"

df = spark.read.parquet(input_path)

# Check the initial number of partitions
print("=== INITIAL PARTITIONS ===")
print(df.rdd.getNumPartitions())


# Cache the DataFrame because we will use it multiple times
df.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger caching
df.count()

print("=== DATAFRAME CACHED ===")
print(df.storageLevel)


# Repartition the data
df = df.repartition(4)

print("=== PARTITIONS AFTER REPARTITION ===")
print(df.rdd.getNumPartitions())

print("=== CLEANED SALES DATA ===")
df.show()

print("=== SCHEMA ===")
df.printSchema()

print("=== TOTAL RECORDS ===")
print(df.count())


# Create temporary view for Spark SQL
df.createOrReplaceTempView("sales")

# Total revenue
total_revenue = spark.sql("""
    SELECT SUM(revenue) AS total_revenue
    FROM sales
""")

print("=== TOTAL REVENUE ===")
total_revenue.show()


# Revenue by product
revenue_by_product = spark.sql("""
    SELECT
        product,
        SUM(revenue) AS total_revenue
    FROM sales
    GROUP BY product
    ORDER BY total_revenue DESC
""")

print("=== REVENUE BY PRODUCT ===")
revenue_by_product.show()


# Revenue by category
revenue_by_category = spark.sql("""
    SELECT
        category,
        SUM(revenue) AS total_revenue
    FROM sales
    GROUP BY category
    ORDER BY total_revenue DESC
""")

print("=== REVENUE BY CITY ===")
# Revenue by city
revenue_by_city = spark.sql("""
    SELECT
        city,
        SUM(revenue) AS total_revenue
    FROM sales
    GROUP BY city
    ORDER BY total_revenue DESC
""")

print("=== REVENUE BY CITY ===")
revenue_by_city.show()


# Product performance analysis
product_performance = spark.sql("""
    SELECT
        product,
        SUM(quantity) AS total_quantity_sold,
        SUM(revenue) AS total_revenue
    FROM sales
    GROUP BY product
    ORDER BY total_revenue DESC
""")

print("=== PRODUCT PERFORMANCE ===")
product_performance.show()

# Save analytics results as Parquet

revenue_by_product.write.mode("overwrite").parquet(
    "/home/neon/ddp-output/revenue_by_product"
)

revenue_by_category.write.mode("overwrite").parquet(
    "/home/neon/ddp-output/revenue_by_category"
)

revenue_by_city.write.mode("overwrite").parquet(
    "/home/neon/ddp-output/revenue_by_city"
)

product_performance.write.mode("overwrite").parquet(
    "/home/neon/ddp-output/product_performance"
)

print("=== ANALYTICS RESULTS SAVED ===")

spark.stop()


