from pyspark.sql import functions as F
from database import read_table


def task_04(spark):

    inventory_df = read_table(spark, "inventory")
    film_df = read_table(spark, "film")

    joined_df = (
        film_df
        .join(inventory_df, on="film_id", how="left_anti")
    )

    result_df = (
        joined_df
        .select("title")
        .orderBy("title")
    )

    result_df.show()
