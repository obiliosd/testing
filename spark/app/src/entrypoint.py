from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json 

# Define the schema to speed up processing
jsonSchema = StructType(
    [StructField("value0", StringType(), True), 
    StructField("value1", ArrayType(DoubleType()), True)])

def get_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession\
        .builder\
        .appName(app_name)\
        .master("spark://spark-master:7077")\
        .config("spark.executor.memory", "512m")\
        .getOrCreate()
    return spark

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    if df.count() <= 0:
        None
    else:
        # Create a data frame to be written to HDFS
        sensor_df = df.selectExpr('CAST(value AS STRING)').select(from_json("value", jsonSchema).alias("value")).select("value.*")

def run_spark_kafka_structure_stream(spark) -> None:
    #Test conexion kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .load()
        #.option("kafka.request.timeout.ms", "60000") \
        #.option("kafka.session.timeout.ms", "30000") \
        #.option("failOnDataLoss", "true") \
        #.option("startingOffsets", "latest") \
        #.load()
        
    words = df.selectExpr('CAST(value AS STRING)') \
        .select(from_json("value", jsonSchema).alias("value")) \
        .select("value.value0")
    
    # Generate running word count
    wordCounts = words.groupBy("value0").count()

     # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()
    """
    """
    # run
    #writer = df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

if __name__ == "__main__":
    #Test 1
    spark = get_spark_session("Stream-kafka")
    run_spark_kafka_structure_stream(spark)
    spark.stop()

