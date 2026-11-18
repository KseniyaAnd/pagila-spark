from pyspark.sql import functions as F
from database import read_table


def task_04(spark):
    """
    Task 04: Output the names of movies that are not in the inventory. 

    Description:
        Retrieve film and inventory data.

        Use a left anti join to find movies from the film table
        that do not have a matching record in the inventory table.

        Select the movie titles and sort them alphabetically.

    Returns:
        None: Displays the resulting DataFrame.
    """

    # 1. Read the required tables
    inventory_df = read_table(spark, "inventory")
    film_df = read_table(spark, "film")

    # 2. Find movies that are not present in the inventory
    joined_df = (
        film_df
        .join(inventory_df, on="film_id", how="left_anti")
    )

    # 3. Select movie titles and sort them alphabetically
    result_df = (
        joined_df
        .select("title")
        .orderBy("title")
    )

    # 4. Display the result
    result_df.show()