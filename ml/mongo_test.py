from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Connection URIs (read & write can be the same)
READ_URI  = "mongodb://127.0.0.1:27017/timeseries_demo.readings"
WRITE_URI = READ_URI

spark = (SparkSession.builder
  .appName("mongo-docker-test")
  .config("spark.mongodb.read.connection.uri", READ_URI)
  .config("spark.mongodb.write.connection.uri", WRITE_URI)
  .getOrCreate())

# --- Write a few rows ---
to_write = spark.createDataFrame([
    ("s-3", 23.1),
    ("s-3", 22.7),
    ("s-4", 20.4),
], ["sensorId", "value"]).withColumn("ts", current_timestamp())

(to_write.write
    .format("mongodb")
    .mode("append")
    .save())

# --- Read them back ---
df = (spark.read
      .format("mongodb")
      .load())

df.show(truncate=False)

# Example: filter by sensor & add a derived column
(df.where(df.sensorId == "s-3")
   .withColumn("source", lit("spark"))
   .show(truncate=False))

spark.stop()