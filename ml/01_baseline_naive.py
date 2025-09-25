# ml/01_baseline_naive.py
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag
from pyspark.ml.evaluation import RegressionEvaluator

def norm_month(s: str):
    return (s if "_" in s else s.replace("-", "_"))

if __name__ == "__main__":
    month = sys.argv[1] if len(sys.argv) > 1 else "2015_01"
    month_u = norm_month(month)

    spark = SparkSession.builder.appName(f"Baseline Naive {month_u}").getOrCreate()

    df = spark.read.parquet(f"data/fact/{month_u}")

    # lag24
    w = Window.partitionBy("zone_id").orderBy("ts_hour")
    df = df.withColumn("yhat", lag("pickups", 24).over(w)).na.drop(subset=["yhat"])

    # cast double
    df = df.withColumn("pickups_d", col("pickups").cast("double")) \
           .withColumn("yhat_d", col("yhat").cast("double"))

    # split temporale
    df = df.withColumn("ts_long", col("ts_hour").cast("long"))
    cut = df.approxQuantile("ts_long", [0.8], 0.0)[0]
    train = df.filter(col("ts_long") <= cut)
    test  = df.filter(col("ts_long") >  cut)

    # RMSE
    e = RegressionEvaluator(labelCol="pickups_d", predictionCol="yhat_d", metricName="rmse")
    rmse = e.evaluate(test)
    print(f"[{month_u}] Baseline Naive RMSE: {rmse:.2f}")

    spark.stop()
