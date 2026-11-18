import os
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()


# PostgreSQL connection settings
JDBC_URL = os.getenv("JDBC_URL")

CONNECTION_PROPERTIES = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER")
}


def read_table(spark, table_name):
    """
    Read a table from PostgreSQL using JDBC.

    Args:
        spark: Active SparkSession.
        table_name: Name of the PostgreSQL table.

    Returns:
        DataFrame: Loaded Spark DataFrame.
    """
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"public.{table_name}",
        properties=CONNECTION_PROPERTIES
    )