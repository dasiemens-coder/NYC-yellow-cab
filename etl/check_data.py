from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Check Data").getOrCreate()

# leggi raw
df_raw = spark.read.parquet("data/raw/yellow_tripdata_2015-01.parquet")
raw_count = df_raw.count()

# leggi silver
df_silver = spark.read.parquet("data/silver/2015_01")
silver_count = df_silver.count()

print("Numero righe raw   :", raw_count)
print("Numero righe silver:", silver_count)

if raw_count == silver_count:
    print("same data")
else:
    print("ATTENTION")

spark.stop()

