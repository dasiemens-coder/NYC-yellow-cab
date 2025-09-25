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
    if len(sys.argv) < 4:
        print("Uso: python viz/plot_predictions.py <rf|lr> <YYYY_MM|YYYY-MM> <zone_id>")
        sys.exit(2)

    model = sys.argv[1].lower()  # 'rf' o 'lr'
    month = norm_month(sys.argv[2])
    zone_id = int(sys.argv[3])

    if model not in ("rf", "lr"):
        raise ValueError("model deve essere 'rf' o 'lr'")

    pred_path = f"data/predictions/{model}_{month}"

    spark = SparkSession.builder.appName(f"Plot {model.upper()} {month} z{zone_id}").getOrCreate()
    df = spark.read.parquet(pred_path)

    # uniforma colonne: y_true, y_pred
    # nei tuoi file: 'pickups_d' (double) e 'prediction'
    y_col = "pickups_d" if "pickups_d" in df.columns else ("pickups" if "pickups" in df.columns else None)
    if y_col is None:
        raise ValueError("Colonna target non trovata (attese: pickups_d o pickups).")

    if "prediction" not in df.columns:
        raise ValueError("Colonna 'prediction' non trovata nelle predizioni.")

    df = (df
          .filter(col("zone_id") == zone_id)
          .select("ts_hour", col(y_col).alias("y_true"), col("prediction").alias("y_pred"))
          .orderBy("ts_hour"))

    # porta in pandas per il plotting
    pdf = df.toPandas()
    if pdf.empty:
        raise ValueError(f"Nessun dato per zone_id={zone_id} in {pred_path}")

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
    # diagonale
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

