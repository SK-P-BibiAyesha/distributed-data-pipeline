from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


def test_cleaning_logic():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("DDPCleaningTests")
        .getOrCreate()
    )

    try:
        # Test data contains duplicates, missing values, and invalid values.
        data = [
            ("1001", "C001", " Laptop ", "Electronics", 2, 100),
            ("1001", "C001", "Laptop", "Electronics", 2, 100),
            ("1002", "C002", "Mouse", "Accessories", 0, 50),
            ("1003", "C003", "Keyboard", "Accessories", 3, 20),
            ("1004", None, "Monitor", "Electronics", 1, 200),
        ]

        columns = [
            "order_id",
            "customer_id",
            "product",
            "category",
            "quantity",
            "unit_price",
        ]

        df = spark.createDataFrame(data, columns)

        # Remove duplicate orders.
        df = df.dropDuplicates(["order_id"])

        # Remove rows with missing important values.
        df = df.dropna(
            subset=[
                "order_id",
                "customer_id",
                "product",
                "category",
                "quantity",
                "unit_price",
            ]
        )

        # Keep only positive quantities and prices.
        df = df.filter(
            (col("quantity") > 0) & (col("unit_price") > 0)
        )

        # Clean whitespace and calculate revenue.
        df = (
            df.withColumn("product", trim(col("product")))
            .withColumn(
                "revenue",
                col("quantity") * col("unit_price"),
            )
        )

        assert df.count() == 2

        laptop = df.filter(col("order_id") == "1001").first()
        assert laptop["product"] == "Laptop"
        assert laptop["revenue"] == 200

        keyboard = df.filter(col("order_id") == "1003").first()
        assert keyboard["revenue"] == 60

    finally:
        spark.stop()


def test_saved_analytics_outputs():
    output_dir = Path.home() / "ddp-output"

    expected_outputs = [
        "revenue_by_product",
        "revenue_by_category",
        "revenue_by_city",
        "product_performance",
    ]

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("DDPOutputTests")
        .getOrCreate()
    )

    try:
        # Check that every analytics output exists and contains data.
        for name in expected_outputs:
            output_path = output_dir / name

            assert output_path.is_dir(), (
                f"Missing analytics output directory: {output_path}"
            )

            df = spark.read.parquet(str(output_path))
            assert df.count() > 0, (
                f"Analytics output is empty: {name}"
            )

        # Verify total revenue by product.
        product_df = spark.read.parquet(
            str(output_dir / "revenue_by_product")
        )

        total_revenue = product_df.agg(
            {"total_revenue": "sum"}
        ).first()[0]

        assert total_revenue == 490600, (
            f"Expected total revenue 490600, got {total_revenue}"
        )

        # Verify the product performance output.
        performance_df = spark.read.parquet(
            str(output_dir / "product_performance")
        )

        performance_revenue = performance_df.agg(
            {"total_revenue": "sum"}
        ).first()[0]

        assert performance_revenue == 490600

    finally:
        spark.stop()


def test_revenue_by_category():
    output_dir = Path.home() / "ddp-output" / "revenue_by_category"

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("DDPCategoryTests")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(output_dir))

        # Convert category revenue results into a dictionary.
        category_revenue = {
            row["category"]: row["total_revenue"]
            for row in df.collect()
        }

        assert category_revenue["Electronics"] == 403000
        assert category_revenue["Furniture"] == 70000
        assert category_revenue["Accessories"] == 17600

    finally:
        spark.stop()
