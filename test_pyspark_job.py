from pyspark.sql import SparkSession

from pyspark_job import process_data


def test_process_data():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTest")
        .getOrCreate()
    )

    result = process_data(spark, "data/input.txt")

    # Check that negative amounts were removed
    assert result.filter("amount <= 0").count() == 0

    # Check the expected number of valid records
    assert result.count() == 25

    # Check the tax calculation
    first_row = result.orderBy("id").first()
    assert first_row["amount_with_tax"] == first_row["amount"] * 1.20

    spark.stop()