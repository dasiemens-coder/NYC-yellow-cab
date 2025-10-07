# viz/plot_predictions.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Uso:
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
#   viz/plot_predictions.py 2015_01 237

def norm_month(s: str) -> str:
    return s if "_" in s else s.replace("-", "_")

def load_preds(spark, collection, month, zone_id):
    """Read predictions from Mongo collection and return a DF(ts_hour, y_true, y_pred)."""
    df = (
        spark.read.format("mongodb")
        .option("database", "timeseries_demo")
        .option("collection", collection)
        .load()
        .filter((col("month") == month) & (col("zone_id") == zone_id))
    )

    # Expect columns: ts_hour, y (actual), y_hat (pred)
    required = {"ts_hour", "y", "y_hat"}
    if not required.issubset(set(df.columns)):
        raise ValueError(
            f"Colonne mancanti in '{collection}'. Attese: {sorted(required)}. "
            "Hai già scritto le predizioni con lo schema corretto?"
        )

    return (
        df.select(
            "ts_hour",
            col("y").alias("y_true"),
            col("y_hat").alias("y_pred")
        ).orderBy("ts_hour")
    )

def main():
    if len(sys.argv) < 3:
        print("Uso: spark-submit ... viz/plot_predictions.py <YYYY_MM|YYYY-MM> <zone_id>")
        sys.exit(2)

    month = norm_month(sys.argv[1])
    zone_id = int(sys.argv[2])

    spark = (
        SparkSession.builder
        .appName(f"Plot RF vs LR {month} z{zone_id}")
        .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017")
        .getOrCreate()
    )

    # Load RF and LR predictions from separate collections
    # (as we set up: readings_pred_rf and readings_pred_lr)
    try:
        df_rf = load_preds(spark, "readings_pred_rf", month, zone_id)
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"Errore nel leggere RF da Mongo: {e}")

    try:
        df_lr = load_preds(spark, "readings_pred_lr", month, zone_id)
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"Errore nel leggere LR da Mongo: {e}")

    pdf_rf = df_rf.toPandas()
    pdf_lr = df_lr.toPandas()

    if pdf_rf.empty:
        spark.stop()
        raise ValueError(f"Nessun dato RF per month={month} zone_id={zone_id} (collezione readings_pred_rf).")
    if pdf_lr.empty:
        spark.stop()
        raise ValueError(f"Nessun dato LR per month={month} zone_id={zone_id} (collezione readings_pred_lr).")

    # Align on ts_hour for plotting (left join on RF's timeline)
    pdf = pdf_rf.merge(
        pdf_lr[["ts_hour", "y_pred"]].rename(columns={"y_pred": "y_pred_lr"}),
        on="ts_hour",
        how="left",
    ).rename(columns={"y_pred": "y_pred_rf"})

    # y_true should be identical between the two; if LR had a differing y_true, prefer RF’s
    # (optionally assert they match where both exist)
    # Plot 1: time series
    plt.figure()
    plt.plot(pdf["ts_hour"], pdf["y_true"], label="y_true")
    plt.plot(pdf["ts_hour"], pdf["y_pred_rf"], label="y_pred_rf")
    plt.plot(pdf["ts_hour"], pdf["y_pred_lr"], label="y_pred_lr")
    plt.legend()
    plt.title(f"RF vs LR — zone {zone_id} — {month}")
    plt.xlabel("ts_hour")
    plt.ylabel("pickups")
    plt.tight_layout()
    plt.show()

    # Plot 2: scatter (two clouds)
    plt.figure()
    plt.scatter(pdf["y_true"], pdf["y_pred_rf"], s=10, alpha=0.5, label="RF")
    plt.scatter(pdf["y_true"], pdf["y_pred_lr"], s=10, alpha=0.5, label="LR")
    lo = min(pdf["y_true"].min(), pdf[["y_pred_rf","y_pred_lr"]].min().min())
    hi = max(pdf["y_true"].max(), pdf[["y_pred_rf","y_pred_lr"]].max().max())
    plt.plot([lo, hi], [lo, hi])
    plt.title(f"RF vs LR — scatter y_true vs y_pred — zone {zone_id} — {month}")
    plt.xlabel("y_true")
    plt.ylabel("y_pred")
    plt.legend()
    plt.tight_layout()
    plt.show()

    spark.stop()

if __name__ == "__main__":
    main()