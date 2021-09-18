# Source code https://docs.microsoft.com/es-es/sql/big-data-cluster/spark-streaming-guide?view=sql-server-ver15
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, from_json 

# Sets up batch size to 15 seconds
duration_ms = 15000
# Changes Spark session into a structured streaming context
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, duration_ms)
spark = SparkSession(sc)

# Connection information
bootstrap_servers = "" # Replace!
sasl = "" # Replace!
# Topic we will consume from
topic = "sample-topic"
# Topic we will write to
topic_to = "sample-topic-processed"

# Define the schema to speed up processing
jsonSchema = StructType([StructField("sensor", StringType(), True), StructField("measure1", DoubleType(), True), StructField("measure2", DoubleType(), True)])

streaming_input_df = (
    spark.readStream \
    .format("kafka") \
    .option("subscribe", topic) \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", sasl) \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.session.timeout.ms", "30000") \
    .option("failOnDataLoss", "true") \
    .option("startingOffsets", "latest") \
    .load()
)

def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    if df.count() <= 0:
        None
    else:
        # Create a data frame to be written to HDFS
        sensor_df = df.selectExpr('CAST(value AS STRING)').select(from_json("value", jsonSchema).alias("value")).select("value.*")
        # root
        #  |-- sensor: string (nullable = true)
        #  |-- measure1: double (nullable = true)
        #  |-- measure2: double (nullable = true)
        sensor_df.persist()
        # Write to HDFS
        sensor_df.write.format('parquet').mode('append').saveAsTable('sensor_data')
        # Create a summarization data frame
        sensor_stats_df = (sensor_df.groupBy('sensor').agg({'measure1':'avg', 'measure2':'avg', 'sensor':'count'}).withColumn('ts', current_timestamp()).withColumnRenamed('avg(measure1)', 'measure1_avg').withColumnRenamed('avg(measure2)', 'measure2_avg').withColumnRenamed('avg(measure1)', 'measure1_avg').withColumnRenamed('count(sensor)', 'count_sensor'))
        # root
        # |-- sensor: string (nullable = true)
        # |-- measure2_avg: double (nullable = true)
        # |-- measure1_avg: double (nullable = true)
        # |-- count_sensor: long (nullable = false)
        # |-- ts: timestamp (nullable = false)
        sensor_stats_df.write.format('parquet').mode('append').saveAsTable('sensor_data_stats')
        # Group by and send metrics to an output Kafka topic
        sensor_stats_df.writeStream \
            .format("kafka") \
            .option("topic", topic_to) \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.jaas.config", sasl) \
            .save()
        # For example, you could write to SQL Server
        # df.write.format('com.microsoft.sqlserver.jdbc.spark').mode('append').option('url', url).option('dbtable', datapool_table).save()
        sensor_df.unpersist()


writer = streaming_input_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()