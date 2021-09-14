from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def get_spark_context(app_name: str) -> SparkSession:
    return SparkSession\
        .builder\
        .appName(app_name)\
        .master("spark://spark-master:7077")\
        .config("spark.executor.memory", "512m")\
        .getOrCreate()

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
    #Test 1
    spark = get_spark_context("cada de la vaca paca")
    run_spark_example(spark)

    #Test conexion kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    print(df)
    print("pingocha peluda")

    spark.stop()