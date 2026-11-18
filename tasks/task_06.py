
from pyspark.sql import functions as F
from database import read_table


def task_06(spark):

    customer_df = read_table(spark, "customer")
    address_df = read_table(spark, "address")
    city_df = read_table(spark, "city")

    joined_df = (
        customer_df
        .join(address_df, on="address_id", how="inner")
        .join(city_df, on="city_id", how="inner")
    )

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
        .orderBy(F.col("inactive_customers").desc())
    )

    result_df.show()
