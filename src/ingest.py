from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("DistributedDataPipeline")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/sales.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=== SALES DATA ===")
df.show()

print("=== SCHEMA ===")
df.printSchema()

print("=== TOTAL RECORDS ===")
print(df.count())

spark.stop()