from pyspark.sql import functions as F
from database import read_table


def task_03(spark):
    """
    Task 03: Output the category of movies on which the most money was spent. 

    Description:
        Retrieve payment, rental, inventory, film_category, and
        category data.

        Join the tables to connect payments with the categories
        of rented movies.

        Calculate the total amount spent on rentals in each
        category, sort the results in descending order by total
        amount, and select the category with the highest revenue.

    Returns:
        None: Displays the resulting DataFrame.
    """

    # 1. Read the required tables
    payment_df = read_table(spark, "payment")
    rental_df = read_table(spark, "rental")
    inventory_df = read_table(spark, "inventory")
    film_category_df = read_table(spark, "film_category")
    category_df = read_table(spark, "category")

    # 2. Join payments with rentals, inventory, and movie categories
    joined_df = (
        payment_df
        .join(rental_df, on="rental_id", how="inner")
        .join(inventory_df, on="inventory_id", how="inner")
        .join(film_category_df, on="film_id", how="inner")
        .join(category_df, on="category_id", how="inner")
    )

    # 3. Calculate total revenue for each category
    # Sort by total amount and select the category with the highest revenue
    result_df = (
        joined_df
        .groupBy("name")
        .agg(F.sum("amount").alias("total_amount"))
        .orderBy(F.col("total_amount").desc())
        .limit(1)
    )

    # 4. Display the result
    result_df.show()