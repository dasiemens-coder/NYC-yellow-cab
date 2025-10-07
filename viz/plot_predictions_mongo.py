# viz/plot_predictions.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Uso:
# python viz/plot_predictions.py rf 2015_01 237
# python viz/plot_predictions.py lr 2015_01 237

def norm_month(s: str) -> str:
    return s if "_" in s else s.replace("-", "_")

def main():
    if len(sys.argv) < 3:
        print("Uso: python viz/plot_predictions.py <rf|lr> <YYYY_MM|YYYY-MM> <zone_id>")
        sys.exit(2)

    model = sys.argv[1].lower()  # 'rf' o 'lr' (solo per titolo)
    month = norm_month(sys.argv[2])
    zone_id = int(sys.argv[3]) if len(sys.argv) > 3 else None

    if model not in ("rf", "lr"):
        raise ValueError("model deve essere 'rf' o 'lr'")

    # SparkSession with Mongo read URI
    spark = (
        SparkSession.builder
        .appName(f"Plot {model.upper()} {month} z{zone_id if zone_id is not None else 'ALL'}")
        .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017")
        .getOrCreate()
    )

    # Read predictions from MongoDB
    # Collection schema (as saved earlier): zone_id, ts_hour, y (actual), y_hat (pred), month, split_q
    df = (
        spark.read
        .format("mongodb")
        .option("database", "timeseries_demo")
        .option("collection", "readings_pred")
        .load()
        .filter(col("month") == month)
    )

    if zone_id is not None:
        df = df.filter(col("zone_id") == zone_id)

    # Expect columns y (actual) and y_hat (prediction)
    if "y" not in df.columns or "y_hat" not in df.columns or "ts_hour" not in df.columns:
        raise ValueError(
            "Colonne attese non trovate in Mongo. Attese: ts_hour, y, y_hat (+ month, zone_id).\n"
            "Hai già scritto le predizioni in timeseries_demo.readings_pred?"
        )

    df = (df
          .select("ts_hour", col("y").alias("y_true"), col("y_hat").alias("y_pred"))
          .orderBy("ts_hour"))

    # to pandas for plotting
    pdf = df.toPandas()
    if pdf.empty:
        raise ValueError(f"Nessun dato in Mongo per month={month} zone_id={zone_id}")

    # --- grafico 1: serie temporale ---
    plt.figure()
    plt.plot(pdf["ts_hour"], pdf["y_true"], label="y_true")
    plt.plot(pdf["ts_hour"], pdf["y_pred"], label="y_pred")
    plt.legend()
    plt.title(f"{model.upper()} — zone {zone_id} — {month}")
    plt.xlabel("ts_hour")
    plt.ylabel("pickups")
    plt.tight_layout()
    plt.show()

    # --- grafico 2: scatter y_true vs y_pred ---
    plt.figure()
    plt.scatter(pdf["y_true"], pdf["y_pred"], s=10, alpha=0.5)
    lo = min(pdf["y_true"].min(), pdf["y_pred"].min())
    hi = max(pdf["y_true"].max(), pdf["y_pred"].max())
    plt.plot([lo, hi], [lo, hi])
    plt.title(f"{model.upper()} — scatter y_true vs y_pred — zone {zone_id} — {month}")
    plt.xlabel("y_true")
    plt.ylabel("y_pred")
    plt.tight_layout()
    plt.show()

    spark.stop()

if __name__ == "__main__":
    main()