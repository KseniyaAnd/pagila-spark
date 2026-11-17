from pyspark.sql import functions as F
from database import read_table


def task_01(spark):

    category_df = read_table(spark, "category")
    film_category_df = read_table(spark, "film_category")

    joined_df = film_category_df.join(
        category_df,
        on="category_id",
        how="inner"
    )

    result_df = (
        joined_df
        .groupBy("name")
        .agg(F.count("film_id").alias("film_count"))
        .orderBy(F.col("film_count").desc())
    )

    result_df.show()