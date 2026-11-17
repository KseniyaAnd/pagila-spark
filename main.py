from spark import create_spark_session

from tasks.task_01 import task_01
from tasks.task_02 import task_02


spark = create_spark_session()

try:

    task_01(spark)
    task_02(spark)

finally:

    spark.stop()