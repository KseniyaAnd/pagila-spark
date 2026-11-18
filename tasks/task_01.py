from pyspark.sql import functions as F
from database import read_table


def task_01(spark):
    """
    Task 01: Output the number of movies in each category, sorted in descending order. 

    Description:
        Retrieve movie categories and their corresponding movie IDs.
        Join the category and film_category tables, count the number
        of movies in each category, and sort the results in descending
        order by movie count.

    Returns:
        None: Displays the resulting DataFrame.
    """

    # 1. Read the required tables
    category_df = read_table(spark, "category")
    film_category_df = read_table(spark, "film_category")

    # 2. Join categories with their movies
    joined_df = film_category_df.join(
        category_df,
        on="category_id",
        how="inner"
    )

    # 3. Count movies in each category and sort by count
    result_df = (
        joined_df
        .groupBy("name")
        .agg(F.count("film_id").alias("film_count"))
        .orderBy(F.col("film_count").desc())
    )

    # 4. Display the result
    result_df.show()