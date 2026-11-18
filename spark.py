from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create and return a SparkSession for the Pagila project.

    Description:
        Initialize a SparkSession using the application name
        "PagilaAnalytics".

        Use getOrCreate() to reuse an existing SparkSession
        if one is already running, or create a new one.

    Returns:
        SparkSession: An active SparkSession instance.
    """

    spark = (
        SparkSession.builder
        .appName("PagilaAnalytics")
        .getOrCreate()
    )

    return spark