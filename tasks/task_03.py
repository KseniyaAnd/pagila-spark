
from pyspark.sql import functions as F
from database import read_table

def task_03(spark):

    payment_df = read_table(spark, "payment")
    rental_df = read_table(spark, "rental")
    inventory_df = read_table(spark, "inventory")
    film_category_df = read_table(spark, "film_category")
    category_df = read_table(spark, "category")

    joined_df = (
        payment_df
        .join(rental_df, on="rental_id", how="inner")
        .join(inventory_df, on="inventory_id", how="inner")
        .join(film_category_df, on="film_id", how="inner")
        .join(category_df, on="category_id", how="inner")
    )

    result_df = (
        joined_df
        .groupBy("name")
        .agg(F.sum("amount").alias("total_amount"))
        .orderBy(F.col("total_amount").desc())
        .limit(1)
    )

    result_df.show()
