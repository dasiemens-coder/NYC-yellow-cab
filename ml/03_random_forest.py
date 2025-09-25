import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def norm_month(s: str) -> str:
    # ritorna 'YYYY_MM' accettando 'YYYY-MM'
    return s if "_" in s else s.replace("-", "_")

if __name__ == "__main__":
    month_u = norm_month(sys.argv[1] if len(sys.argv) > 1 else "2015_01")
    split_q = float(sys.argv[2]) if len(sys.argv) > 2 else 0.8  # quantile per cutoff temporale

    spark = SparkSession.builder.appName(f"RF with Lag {month_u}").getOrCreate()

    # 1) Carica GOLD_LAG del mese
    df = spark.read.parquet(f"data/gold_lag/{month_u}")

    # 2) Seleziona le feature (usa solo quelle presenti)
    base_features = ["hour","dow","is_weekend","lag1","lag24","lag168","roll24_mean","roll168_mean"]
    present = [c for c in base_features if c in df.columns]
    # Richiedi almeno le lag principali
    for req in ["lag1","lag24","lag168"]:
        if req not in present:
            raise ValueError(f"Manca la colonna obbligatoria {req}. Esegui etl/04_build_lag_features.py")

    needed = ["zone_id","ts_hour","pickups"] + present
    df = df.select(*needed).na.drop(subset=["lag1","lag24","lag168"])

    # 3) Label a double
    df = df.withColumn("pickups_d", col("pickups").cast("double"))

    # 4) Split temporale (serve numerico per approxQuantile)
    df = df.withColumn("ts_long", col("ts_hour").cast("long"))
    cut = df.approxQuantile("ts_long", [split_q], 0.0)[0]
    train = df.filter(col("ts_long") <= cut)
    test  = df.filter(col("ts_long") >  cut)

    # 5) Assembler
    assembler = VectorAssembler(inputCols=present, outputCol="features")
    train_v = assembler.transform(train).select("zone_id","ts_hour","pickups_d","features")
    test_v  = assembler.transform(test ).select("zone_id","ts_hour","pickups_d","features")

    # 6) RandomForest
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="pickups_d",
        numTrees=200,
        maxDepth=12,
        minInstancesPerNode=2,
        subsamplingRate=0.8,
        featureSubsetStrategy="auto",
        seed=42
    )
    model = rf.fit(train_v)

    # 7) Predizioni + RMSE
    pred = model.transform(test_v)
    evaluator = RegressionEvaluator(labelCol="pickups_d", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(pred)
    has_rolling = {"roll24_mean","roll168_mean"}.issubset(set(present))
    print(f"[{month_u}] RandomForest (calendar+lag{' + rolling' if has_rolling else ''}) RMSE: {rmse:.2f}")

    # 8) Feature importances
    importances = list(model.featureImportances)
    print("Feature importances:")
    for name, imp in zip(present, importances):
        print(f"  {name}: {imp:.6f}")

    # 9) Salvataggi
    pred_out = f"data/predictions/rf_{month_u}"
    model_out = f"data/models/rf_{month_u}"
    pred.select("zone_id","ts_hour","pickups_d","prediction") \
        .orderBy("zone_id","ts_hour") \
        .write.mode("overwrite").parquet(pred_out)
    model.write().overwrite().save(model_out)

    spark.stop()
