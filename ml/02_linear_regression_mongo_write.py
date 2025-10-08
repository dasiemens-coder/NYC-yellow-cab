import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def norm_month(s: str) -> str:
    """Ritorna 'YYYY_MM' accettando 'YYYY-MM'."""
    return s if "_" in s else s.replace("-", "_")

if __name__ == "__main__":
    # Parametri
    month_u = norm_month(sys.argv[1] if len(sys.argv) > 1 else "2015_01")
    split_q = float(sys.argv[2]) if len(sys.argv) > 2 else 0.8  # quantile per il cutoff temporale (0–1)

    spark = (
        SparkSession.builder
        .appName(f"LR with Lag {month_u}")
        # Base URI (DB/collection specified per-write)
        .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017")
        .getOrCreate()
    )

    # 1) Carica GOLD_LAG del mese
    df = spark.read.parquet(f"data/gold_lag/{month_u}")

    # 2) Seleziona feature disponibili
    base_features = ["hour","dow","is_weekend","lag1","lag24","lag168","roll24_mean","roll168_mean"]
    present = [c for c in base_features if c in df.columns]
    if not {"lag1","lag24","lag168"}.issubset(set(present)):
        raise ValueError("Mancano le colonne lag obbligatorie: lag1, lag24, lag168. Esegui etl/04_build_lag_features.py")

    needed = ["zone_id","ts_hour","pickups"] + present
    df = df.select(*needed).na.drop(subset=["lag1","lag24","lag168"])

    # 3) Label a double
    df = df.withColumn("pickups_d", col("pickups").cast("double"))

    # 4) Split temporale (serve numerico)
    df = df.withColumn("ts_long", col("ts_hour").cast("long"))
    cut = df.approxQuantile("ts_long", [split_q], 0.0)[0]
    train = df.filter(col("ts_long") <= cut)
    test  = df.filter(col("ts_long") >  cut)

    # 5) Assembler + LR
    assembler = VectorAssembler(inputCols=present, outputCol="features")
    train_v = assembler.transform(train).select("zone_id","ts_hour","pickups_d","features")
    test_v  = assembler.transform(test ).select("zone_id","ts_hour","pickups_d","features")

    lr = LinearRegression(featuresCol="features", labelCol="pickups_d")
    model = lr.fit(train_v)

    # 6) Valutazione
    pred = model.transform(test_v)
    evaluator = RegressionEvaluator(labelCol="pickups_d", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(pred)
    print(f"[{month_u}] Linear Regression (calendar+lag{'+rolling' if set(['roll24_mean','roll168_mean']).issubset(set(present)) else ''}) RMSE: {rmse:.2f}")


    # 8) Save Metrics in MongoDB
    metrics_df = spark.createDataFrame(
        [(month_u, split_q, float(rmse), "LinearRegression",
          set(['roll24_mean','roll168_mean']).issubset(set(present)))],
        ["month", "split_q", "rmse", "model", "used_rolling"]
    )
    (metrics_df.write
        .format("mongodb")
        .mode("append")
        .option("database", "timeseries_demo")
        .option("collection", "metrics")
        .save())

    # 9) Save Predictions in MongoDB
    (pred.select(
            "zone_id",
            "ts_hour",                        # Spark Timestamp -> Mongo Date
            col("pickups_d").alias("y"),      # actual
            col("prediction").alias("y_hat")  # predicted
        )
        .withColumn("month", lit(month_u))
        .withColumn("split_q", lit(split_q))
        .withColumn("model", lit("lr"))
        .write
        .format("mongodb")
        .mode("append")
        .option("database", "timeseries_demo")
        .option("collection", "readings_pred_lr")
        .save())

    spark.stop()