from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Create Spark session
spark = (
    SparkSession.builder
    .appName("DataTransformation")
    .master("local[*]")
    .getOrCreate()
)

# Read raw sales data
input_path = "data/raw/sales.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== ORIGINAL DATA ===")
df.show()

# Calculate revenue
df = df.withColumn(
    "revenue",
    col("quantity") * col("unit_price")
)

print("=== DATA WITH REVENUE ===")
df.show()

# Show the new schema
print("=== UPDATED SCHEMA ===")
df.printSchema()

spark.stop()