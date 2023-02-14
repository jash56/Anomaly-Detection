# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 inference.py

from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import pickle
import pandas as pd
from sklearn.ensemble import IsolationForest

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("kafka_data_publisher")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

streaming_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers",'localhost:9092')\
        .option("failOnDataLoss",False)\
        .option("subscribe", 'SKAB')\
        .option("startingOffsets", "earliest")\
        .option("maxOffsetsPerTrigger", 100)\
        .load()

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

streaming_df = streaming_df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), df_schema))\
    .select("from_json(value).*")

streaming_df = streaming_df.toDF(*(c.replace('.', '_') for c in streaming_df.columns))

model = pickle.load(open("models/isolation_forest.sav", 'rb'))
threshold = 0.64
def run_inference(data, epoch_id):
    df = data.toPandas()
    print(df.head(5))
    df['anomaly_score'] = model.score_samples(df.drop(['datetime', 'anomaly','changepoint'], axis=1))
    anomalous_data = df[df.loc[df.anomaly_score > threshold]]
    anomalous_data.to_parquet(path ="results/" + str(epoch_id))

query1 = (streaming_df\
.writeStream\
.format("console")\
.outputMode("append")\
.foreachBatch(run_inference)\
.start())

spark.streams.awaitAnyTermination()