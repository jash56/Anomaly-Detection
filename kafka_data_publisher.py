# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 kafka_data_publisher.py

from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("kafka_data_publisher")\
    .getOrCreate()

df_schema = StructType([
    StructField(name = 'datetime', dataType = StringType(), nullable = False),
    StructField(name = 'Accelerometer1RMS', dataType = DoubleType(), nullable = False),
    StructField(name = 'Accelerometer2RMS', dataType = DoubleType(), nullable = False),
    StructField(name = 'Current', dataType = DoubleType(), nullable = False),
    StructField(name = 'Pressure', dataType = DoubleType(), nullable = False),
    StructField(name = 'Temperature', dataType = DoubleType(), nullable = False),
    StructField(name = 'Thermocouple', dataType = DoubleType(), nullable = False),
    StructField(name = 'Voltage', dataType = DoubleType(), nullable = False),
    StructField(name = 'Volume Flow RateRMS', dataType = DoubleType(), nullable = False),
    StructField(name = 'anomaly', dataType = DoubleType(), nullable = False),
    StructField(name = 'changepoint', dataType = DoubleType(), nullable = False),
])

df = spark\
    .read\
    .option("header", "true")\
    .option("sep", ";")\
    .schema(df_schema)\
    .csv('SKAB*.csv')\
    .withColumn('value', to_json(struct(col("*"))))\
    .select("value")

df\
.write\
.format("kafka")\
.option("kafka.bootstrap.servers", 'localhost:9092')\
.option("topic", 'SKAB')\
.save()

spark.streams.awaitAnyTermination()
# spark.stop()