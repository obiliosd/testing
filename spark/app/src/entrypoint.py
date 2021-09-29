from pyspark.sql import SparkSession

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"

def get_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession\
        .builder\
        .appName(app_name)\
        .master("spark://spark-master:7077")\
        .config("spark.executor.memory", "512m")\
        .config("spark.cassandra.connection.host", "cassandra")\
        .getOrCreate()
    return spark

def load_and_get_table_df(spark, keys_space_name, table_name):
    table_df = spark.read\
        .format(CASSANDRA_FORMAT)\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df

def write_table_df(table_df, keys_space_name, table_name):
    table_df.write\
        .format(CASSANDRA_FORMAT)\
        .mode('append')\
        .options(table=table_name, keyspace=keys_space_name)\
        .save()

if __name__ == "__main__":
    spark = get_spark_session("cassandra")
    keys_space_name = "sparkstreaming"
    table_name = "estudiantes"
    
    print("Cargando tabla estudiantes y mostrándola:")
    estudiantes = load_and_get_table_df(spark, keys_space_name, table_name)
    estudiantes.show()
    
    print("Actualizando tabla test:")
    table_name = "test"
    df = spark.createDataFrame(
    [
        ("1", "fooOOO"),  # create your data here, be consistent in the types.
        ("2", "barRRR"),
        ("3", "test"),
    ],
    ["identificador", "nombre"]  # add your column names here
    )
    df.show()
    write_table_df(df, keys_space_name, table_name)
    
    spark.stop()


