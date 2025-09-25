import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

    spark = SparkSession.builder.appName(f"LR with Lag {month_u}").getOrCreate()

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

    # 7) Output (prime righe) e salvataggi
    pred.select("zone_id","ts_hour","pickups_d","prediction") \
        .orderBy("zone_id","ts_hour").show(20, truncate=False)

    # salva predizioni e modello
    pred.write.mode("overwrite").parquet(f"data/predictions/lr_{month_u}")
    model.save(f"data/models/lr_{month_u}")

    spark.stop()
