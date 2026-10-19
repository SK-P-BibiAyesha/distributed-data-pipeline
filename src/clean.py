import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


# Create Spark session
spark = (
    SparkSession.builder
    .appName("DataCleaning")
    .master("local[*]")
    .getOrCreate()
)

# Read messy sales data
input_path = "data/raw/sales_messy.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== ORIGINAL MESSY DATA ===")
df.show()

print("=== ORIGINAL RECORD COUNT ===")
print(df.count())


# Remove duplicate orders
df = df.dropDuplicates(["order_id"])


# Remove rows with missing important values
df = df.dropna(
    subset=[
        "order_id",
        "customer_id",
        "product",
        "category",
        "quantity",
        "unit_price",
        "order_date",
        "city"
    ]
)


# Keep only valid quantities and prices
df = df.filter(
    (col("quantity") > 0) &
    (col("unit_price") > 0)
)


# Remove extra spaces from text columns
df = (
    df.withColumn("product", trim(col("product")))
      .withColumn("category", trim(col("category")))
      .withColumn("city", trim(col("city")))
)


# Calculate revenue
df = df.withColumn(
    "revenue",
    col("quantity") * col("unit_price")
)


print("=== CLEANED DATA ===")
df.show()

print("=== CLEANED RECORD COUNT ===")
print(df.count())

print("=== CLEANED SCHEMA ===")
df.printSchema()

# Save cleaned data as Parquet using PyArrow
output_path = "data/processed/clean_sales.parquet"

cleaned_pandas_df = df.toPandas()

table = pa.Table.from_pandas(cleaned_pandas_df)
pq.write_table(table, output_path)

print("=== CLEANED DATA SAVED ===")
print(f"Output path: {output_path}")

spark.stop()