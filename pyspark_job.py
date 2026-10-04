from pyspark.sql import functions as F


def process_data(spark, input_path):
    df = (
        spark.read
        .option("sep", ",")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    df = df.toDF("id", "name", "amount")

    result = df.filter(
        F.col("amount") > 0
    )

    result = result.withColumn(
        "amount_with_tax",
        F.col("amount") * 1.20
    )

    return result