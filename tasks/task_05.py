from pyspark.sql import functions as F
from pyspark.sql.window import Window
from database import read_table


def task_05(spark):
    """
    Task 05: Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them. 

    Description:
        Retrieve actor, film_actor, film_category, and category data.

        Join the tables to identify actors who appeared in movies
        belonging to the "Children" category.

        Count the number of distinct movies for each actor.
        Use dense_rank() to rank actors by their movie count
        in descending order.

        Select the top 3 ranks. If several actors have the same
        number of movies, include all actors with that rank.

        Sort the results by rank, last name, and first name.

    Returns:
        None: Displays the resulting DataFrame.
    """

    # 1. Read the required tables
    actor_df = read_table(spark, "actor")
    film_actor_df = read_table(spark, "film_actor")
    film_category_df = read_table(spark, "film_category")
    category_df = read_table(spark, "category")

    # 2. Join actors with movies and their categories
    joined_df = (
        actor_df
        .join(film_actor_df, on="actor_id", how="inner")
        .join(film_category_df, on="film_id", how="inner")
        .join(category_df, on="category_id", how="inner")
    )

    # 3. Filter movies belonging to the Children category
    children_df = joined_df.filter(
        F.col("name") == "Children"
    )

    # 4. Count distinct movies for each actor
    actor_counts_df = (
        children_df
        .groupBy("actor_id", "first_name", "last_name")
        .agg(
            F.countDistinct("film_id").alias("film_count")
        )
    )

    # 5. Create a window and rank actors by movie count
    window = Window.orderBy(
        F.col("film_count").desc()
    )

    ranked_df = actor_counts_df.withColumn(
        "actor_rank",
        F.dense_rank().over(window)
    )

    # 6. Select the top 3 ranks, including ties
    result_df = (
        ranked_df
        .filter(F.col("actor_rank") <= 3)
        .orderBy("actor_rank", "last_name", "first_name")
        .select("first_name", "last_name", "film_count")
    )

    # 7. Display the result
    result_df.show()