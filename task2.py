import os
os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-21.0.10.7-hotspot"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["SPARK_HOME"] = "C:/Users/Medha K/spark-4.1.1-bin-hadoop3/spark-4.1.1-bin-hadoop3"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, sum as spark_sum, avg

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAggregations").config("spark.sql.shuffle.partitions", "4").getOrCreate()
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

# Compute aggregations: total fare and average distance grouped by driver_id
agg_df = parsed_df.groupBy("driver_id").agg(
    spark_sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file
def write_batch(batch_df, batch_id):
    print(f"[write_batch] batch_id={batch_id}, count={batch_df.count()}")
    batch_df.show()
    if batch_df.count() == 0:
        print("[write_batch] Empty batch, skipping write.")
        return
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    output_path = f"c:/Users/Medha K/Hands-on-L9-Streaming-Analytics-with-Spark/outputs/task_2/batch_{batch_id}"
    try:
        batch_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"[write_batch] Written to {output_path}")
    except Exception as e:
        print(f"[write_batch] ERROR: {e}")

# Use foreachBatch to apply the function to each micro-batch
query = (agg_df.writeStream
    .outputMode("complete")
    .foreachBatch(write_batch)
    .trigger(processingTime="10 seconds")
    .start())

query.awaitTermination()
