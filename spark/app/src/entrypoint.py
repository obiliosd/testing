from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def get_spark_context(app_name: str) -> SparkSession:
    return SparkSession.\
        builder.\
        appName(app_name).\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()

def run_spark_example(spark) -> None:
    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StructType
    rows = [
        [1, 100],
        [2, 200],
        [3, 300],
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("salary", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(rows, schema=schema)
    highest_salary = df.agg({"salary": "max"}).collect()[0]["max(salary)"]
    second_highest_salary = (
        df.filter(f"`salary` < {highest_salary}")
        .orderBy("salary", ascending=False)
        .select("salary")
        .limit(1)
    )
    second_highest_salary.show()

if __name__ == "__main__":
    # Regular Spark job executed on a Docker container
    spark = get_spark_context("can of the bac")
    run_spark_example(spark)
    spark.stop()