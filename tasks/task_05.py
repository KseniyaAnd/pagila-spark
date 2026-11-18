
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from database import read_table


def task_05(spark):

    actor_df = read_table(spark, "actor")
    film_actor_df = read_table(spark, "film_actor")
    film_category_df = read_table(spark, "film_category")
    category_df = read_table(spark, "category")

    joined_df = (
        actor_df
        .join(film_actor_df, on="actor_id", how="inner")
        .join(film_category_df, on="film_id", how="inner")
        .join(category_df, on="category_id", how="inner")
    )

    children_df = joined_df.filter(
        F.col("name") == "Children"
    )

    actor_counts_df = (
        children_df
        .groupBy("actor_id", "first_name", "last_name")
        .agg(
            F.countDistinct("film_id").alias("film_count")
        )
    )

    window = Window.orderBy(
        F.col("film_count").desc()
    )

    ranked_df = actor_counts_df.withColumn(
        "actor_rank",
        F.dense_rank().over(window)
    )

    result_df = (
        ranked_df
        .filter(F.col("actor_rank") <= 3)
        .orderBy("actor_rank", "last_name", "first_name")
        .select("first_name", "last_name", "film_count")
    )

    result_df.show()
