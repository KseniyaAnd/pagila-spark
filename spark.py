from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("PagilaPractice")
        .getOrCreate()
    )

    return spark