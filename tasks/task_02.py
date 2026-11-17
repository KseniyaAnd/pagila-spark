from pyspark.sql import functions as F
from database import read_table


def task_02(spark):

    rental_df = read_table(spark, "rental")
    inventory_df = read_table(spark, "inventory")
    film_actor_df = read_table(spark, "film_actor")
    actor_df = read_table(spark, "actor")

    joined_df = (
        rental_df
        .join(inventory_df, on="inventory_id", how="inner")
        .join(film_actor_df, on="film_id", how="inner")
        .join(actor_df, on="actor_id", how="inner")
    )

    result_df = (
        joined_df
        .groupBy("actor_id", "first_name", "last_name")
        .agg(F.count("rental_id").alias("rental_count"))
        .orderBy(F.col("rental_count").desc())
        .limit(10)
    )

    result_df.show()