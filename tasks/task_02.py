from pyspark.sql import functions as F
from database import read_table


def task_02(spark):
    """
    Task 02: Output the 10 actors whose movies rented the most, sorted in descending order. 

    Description:
        Retrieve rental, inventory, film_actor, and actor data.
        Join the tables to connect rentals with the actors who
        appeared in the rented movies.

        Count the number of rentals for each actor, sort the
        results in descending order by rental count, and return
        the top 10 actors whose movies were rented the most.

    Returns:
        None: Displays the resulting DataFrame.
    """

    # 1. Read the required tables
    rental_df = read_table(spark, "rental")
    inventory_df = read_table(spark, "inventory")
    film_actor_df = read_table(spark, "film_actor")
    actor_df = read_table(spark, "actor")

    # 2. Join rentals with inventory, films, and actors
    joined_df = (
        rental_df
        .join(inventory_df, on="inventory_id", how="inner")
        .join(film_actor_df, on="film_id", how="inner")
        .join(actor_df, on="actor_id", how="inner")
    )

    # 3. Count rentals for each actor, sort, and select the top 10
    result_df = (
        joined_df
        .groupBy("actor_id", "first_name", "last_name")
        .agg(F.count("rental_id").alias("rental_count"))
        .orderBy(F.col("rental_count").desc())
        .limit(10)
    )

    # 4. Display the result
    result_df.show()