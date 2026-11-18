"""
Main entry point for the PySpark Pagila project.

Description:
    Create a SparkSession and execute seven PySpark tasks
    using data retrieved from the PostgreSQL Pagila database.

    Each task performs data analysis, including:
        1. Count movies in each category.
        2. Find the 10 most rented actors.
        3. Find the category with the highest rental revenue.
        4. Find movies that are not in the inventory.
        5. Find the top 3 actors in the Children category.
        6. Count active and inactive customers by city.
        7. Find the most rented categories by city group,
           based on total rental hours.

    Stop the SparkSession after all tasks have been executed,
    even if an error occurs.
"""

from spark import create_spark_session

from tasks.task_01 import task_01
from tasks.task_02 import task_02
from tasks.task_03 import task_03
from tasks.task_04 import task_04
from tasks.task_05 import task_05
from tasks.task_06 import task_06
from tasks.task_07 import task_07


def main():
    """
    Create a SparkSession and execute all project tasks.

    Stop the SparkSession in the finally block to ensure
    proper resource cleanup.
    """

    spark = create_spark_session()

    try:
        # Task 01: Count movies in each category
        task_01(spark)

        # Task 02: Find the 10 most rented actors
        task_02(spark)

        # Task 03: Find the category with the highest rental revenue
        task_03(spark)

        # Task 04: Find movies that are not in the inventory
        task_04(spark)

        # Task 05: Find the top 3 actors in the Children category
        task_05(spark)

        # Task 06: Count active and inactive customers by city
        task_06(spark)

        # Task 07: Find the most rented categories by city group
        task_07(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()