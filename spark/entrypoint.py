  
import pyspark
import os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

def get_spark_context(app_name: str) -> SparkSession:
    """
    Helper to manage the `SparkContext` and keep all of our
    configuration params in one place. See below comments for details:
        |_ https://github.com/bitnami/bitnami-docker-spark/issues/18#issuecomment-700628676
        |_ https://github.com/leriel/pyspark-easy-start/blob/master/read_file.py
    """
    conf = SparkConf()
    conf.setAll(
        [
            ("spark.master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")),
            ("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "local[*]")),
            ("spark.submit.deployMode", "client"),
            ("spark.driver.bindAddress", "0.0.0.0"),
            ("spark.app.name", app_name),
        ]
    )
    return SparkSession.builder.config(conf=conf).getOrCreate()

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

def run_spark_print(spark) -> None:
    #sc = pyspark.SparkContext('local[*]')
    sc = spark.sparkContext
    txt = sc.textFile('file:////usr//share//X11//Xcms.txt')
    python_lines = txt.filter(lambda line: 'the' in line.lower())
    print("The number of lines containing 'the' in your file is: ", python_lines.count())

if __name__ == "__main__":
    # Regular Spark job executed on a Docker container
    spark = get_spark_context("employees")
    #run_spark_example(spark)
    run_spark_print(spark)
    spark.stop()