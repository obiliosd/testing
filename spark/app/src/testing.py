from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json
from pyspark.streaming.kafka import KafkaUtils

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

def run_spark_kafka_ds_stream(spark) -> None:
    print("run_spark_kafka_conection - INIT")
    # Initialize SparkContext
    sc = spark.sparkContext
    # Initialize spark stream context
    batchInterval = 5
    ssc = StreamingContext(sc, batchInterval)
    # Set kafka topic
    topic = {"test": 1}
    # Set application groupId
    groupId = "test"
    # Set zookeeper parameter
    zkQuorum = "localhost:2181"
    # Create Kafka stream 
    kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    #Do as you wish with your stream
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
    # Start stream
    ssc.start()
    ssc.awaitTermination()
    print("run_spark_kafka_conection - END")

def get_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession\
        .builder\
        .appName(app_name)\
        .master("spark://spark-master:7077")\
        .config("spark.executor.memory", "512m")\
        .getOrCreate()
    return spark

if __name__ == "__main__":
    #Test 1
    spark = get_spark_session("Stream-kafka")
    run_spark_example(spark)
    run_spark_kafka_ds_stream(spark)
    spark.stop()

