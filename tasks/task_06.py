from pyspark.sql import functions as F
from database import read_table


def task_06(spark):
    """
    Task 06: Output cities with the number of active and inactive customers (active - customer.active = 1). 
             Sort by the number of inactive customers in descending order. 

    Description:
        Retrieve customer, address, and city data.

        Join the tables to associate each customer with their city.

        Group customers by city and calculate the number of active
        and inactive customers.

        A customer is considered active if customer.active = 1
        and inactive if customer.active = 0.

        Sort the results in descending order by the number
        of inactive customers.

    Returns:
        None: Displays the resulting DataFrame.
    """

    # 1. Read the required tables
    customer_df = read_table(spark, "customer")
    address_df = read_table(spark, "address")
    city_df = read_table(spark, "city")

    # 2. Join customers with their addresses and cities
    joined_df = (
        customer_df
        .join(address_df, on="address_id", how="inner")
        .join(city_df, on="city_id", how="inner")
    )

    # 3. Count active and inactive customers in each city
    result_df = (
        joined_df
        .groupBy("city_id", "city")
        .agg(
            F.sum(
                F.when(F.col("active") == 1, 1).otherwise(0)
            ).alias("active_customers"),

            F.sum(
                F.when(F.col("active") == 0, 1).otherwise(0)
            ).alias("inactive_customers")
        )
        # 4. Sort cities by inactive customer count
        .orderBy(F.col("inactive_customers").desc())
    )

    # 5. Display the result
    result_df.show()