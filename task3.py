import os
os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-21.0.10.7-hotspot"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["SPARK_HOME"] = "C:/Users/Medha K/spark-4.1.1-bin-hadoop3/spark-4.1.1-bin-hadoop3"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, sum as spark_sum, window, to_timestamp

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingWindowed").config("spark.sql.shuffle.partitions", "4").getOrCreate()
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

# Convert timestamp column to TimestampType and add a watermark
windowed_df = (parsed_df
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withWatermark("event_time", "1 minute")
    .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
    .agg(spark_sum("fare_amount").alias("total_fare"))
)

# Extract window start and end times as separate columns
result_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included
    output_path = f"c:/Users/Medha K/Hands-on-L9-Streaming-Analytics-with-Spark/outputs/task_3/batch_{batch_id}"
    batch_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Use foreachBatch to apply the function to each micro-batch
query = (result_df.writeStream
    .outputMode("append")
    .foreachBatch(write_batch)
    .trigger(processingTime="10 seconds")
    .start())

query.awaitTermination()
