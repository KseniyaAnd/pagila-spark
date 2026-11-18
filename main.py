from spark import create_spark_session

from tasks.task_01 import task_01
from tasks.task_02 import task_02
from tasks.task_03 import task_03
from tasks.task_04 import task_04
from tasks.task_05 import task_05


spark = create_spark_session()

try:

    task_01(spark)
    task_02(spark)
    task_03(spark)
    task_04(spark)
    task_05(spark)

finally:

    spark.stop()