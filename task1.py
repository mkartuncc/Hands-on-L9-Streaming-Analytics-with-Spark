import os
os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-21.0.10.7-hotspot"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["SPARK_HOME"] = "C:/Users/Medha K/spark-4.1.1-bin-hadoop3/spark-4.1.1-bin-hadoop3"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
init_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9998).load()

# Parse JSON data into columns using the defined schema
parsed_df = init_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Print parsed data to the console
query = (parsed_df.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "c:/Users/Medha K/Hands-on-L9-Streaming-Analytics-with-Spark/outputs/task_1")
    .option("checkpointLocation", "c:/Users/Medha K/Hands-on-L9-Streaming-Analytics-with-Spark/outputs/task_1/checkpoints")
    .option("header", "true")
    .trigger(processingTime="10 seconds")
    .start())

query.awaitTermination()
