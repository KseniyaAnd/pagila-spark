# Настройки подключения к PostgreSQL

JDBC_URL = "jdbc:postgresql://localhost:5433/pagila"

CONNECTION_PROPERTIES = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver"
}

def read_table(spark, table_name):
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"public.{table_name}",
        properties=CONNECTION_PROPERTIES
    )