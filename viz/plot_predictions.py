# viz/plot_predictions_mongo.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import pandas as pd


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

    required = {"ts_hour", "y", "y_hat"}
    if not required.issubset(set(df.columns)):
        raise ValueError(
            f"Missing columns in '{collection}'. Expected: {sorted(required)}. "
            "Have you written predictions with the correct schema?"
        )

    return (
        df.select(
            "ts_hour",
            col("y").alias("y_true"),
            col("y_hat").alias("y_pred")
        ).orderBy("ts_hour")
    )


def load_metrics(spark, month):
    """Read latest-per-model metrics for a month."""
    metrics = (
        spark.read.format("mongodb")
        .option("database", "timeseries_demo")
        .option("collection", "metrics")
        .load()
        .filter(col("month") == month)
    )

    # pick the latest by split_q within each model
    w = Window.partitionBy("model").orderBy(desc("split_q"))
    latest = (
        metrics.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("model", "month", "split_q", "rmse", "used_rolling")
    )
    return latest


def main():
    if len(sys.argv) < 3:
        print("Usage: spark-submit ... viz/plot_predictions_mongo.py <YYYY_MM|YYYY-MM> <zone_id>")
        sys.exit(2)

    month = norm_month(sys.argv[1])
    zone_id = int(sys.argv[2])

    spark = (
        SparkSession.builder
        .appName(f"Plot RF vs LR {month} z{zone_id}")
        .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017")
        .getOrCreate()
    )

    try:
        df_rf = load_preds(spark, "readings_pred_rf", month, zone_id)
        df_lr = load_preds(spark, "readings_pred_lr", month, zone_id)
    except Exception:
        spark.stop()
        raise

    pdf_rf = df_rf.toPandas()
    pdf_lr = df_lr.toPandas()

    if pdf_rf.empty:
        spark.stop()
        raise ValueError(f"No RF data for month={month} zone_id={zone_id} (collection readings_pred_rf).")
    if pdf_lr.empty:
        spark.stop()
        raise ValueError(f"No LR data for month={month} zone_id={zone_id} (collection readings_pred_lr).")

    # Align on ts_hour for plotting (left join on RF's timeline)
    pdf = pdf_rf.merge(
        pdf_lr[["ts_hour", "y_pred"]].rename(columns={"y_pred": "y_pred_lr"}),
        on="ts_hour",
        how="left",
    ).rename(columns={"y_pred": "y_pred_rf"})

    # Ensure ts_hour is datetime for nicer x-axis formatting
    if not pd.api.types.is_datetime64_any_dtype(pdf["ts_hour"]):
        pdf["ts_hour"] = pd.to_datetime(pdf["ts_hour"])

    # Load latest metrics per model
    latest = load_metrics(spark, month)
    pdf_metrics = latest.toPandas()

    # --- Figure layout ---
    fig = plt.figure(figsize=(16, 10), constrained_layout=True)
    gs = GridSpec(2, 2, figure=fig, height_ratios=[3, 2])

    ax_ts = fig.add_subplot(gs[0, 0])   # top-left
    ax_sc = fig.add_subplot(gs[0, 1])   # top-right
    ax_tbl = fig.add_subplot(gs[1, :])  # bottom spans both columns

    # Global header
    fig.suptitle(f"Zone: {zone_id}   Month: {month}", y=1.0, fontsize=14)

    # Plot 1: time series
    line_true, = ax_ts.plot(pdf["ts_hour"], pdf["y_true"], color="black", label="y_true (actual)")
    line_rf,   = ax_ts.plot(pdf["ts_hour"], pdf["y_pred_rf"], color="blue", label="rf (Random Forest)")
    line_lr,   = ax_ts.plot(pdf["ts_hour"], pdf["y_pred_lr"], color="orange", label="lr (Linear Regression)")
    ax_ts.set_title("Time series plot")
    ax_ts.set_xlabel("timestamp")
    ax_ts.set_ylabel("pickups")
    for label in ax_ts.get_xticklabels():
        label.set_rotation(45)
        label.set_ha("right")

    # Plot 2: scatter
    sc_rf = ax_sc.scatter(pdf["y_true"], pdf["y_pred_rf"], s=10, alpha=0.5, label="rf")
    sc_lr = ax_sc.scatter(pdf["y_true"], pdf["y_pred_lr"], s=10, alpha=0.5, label="lr")
    lo = min(pdf["y_true"].min(), pdf[["y_pred_rf", "y_pred_lr"]].min().min())
    hi = max(pdf["y_true"].max(), pdf[["y_pred_rf", "y_pred_lr"]].max().max())
    ax_sc.plot([lo, hi], [lo, hi], linestyle="--", color="gray")
    ax_sc.set_title("Scatter plot")
    ax_sc.set_xlabel("y_true")
    ax_sc.set_ylabel("y_pred")

    # Unified legend (keep actual colors)
    fig.legend(
        handles=[line_true ,line_rf, line_lr],
        labels=["Y_true" ,"Random Forest (rf)", "Linear Regression (lr)"],
        loc="lower center",
        ncol=2,
        bbox_to_anchor=(0.5, 0.02),
        frameon=False
    )

    # Table
    ax_tbl.axis("off")
    if pdf_metrics.empty:
        ax_tbl.text(0.5, 0.5, "No metrics found for this month.", ha="center", va="center", fontsize=12)
        ax_tbl.set_title("Latest metrics", pad=14)
    else:
        table_data = pdf_metrics[["model", "rmse"]].copy()
        table_data["rmse"] = table_data["rmse"].round(2)
        the_table = ax_tbl.table(
            cellText=table_data.values,
            colLabels=["Model", "RMSE"],
            cellLoc="center",
            loc="center",
        )
        the_table.auto_set_font_size(False)
        the_table.set_fontsize(10)
        the_table.auto_set_column_width(col=list(range(len(table_data.columns))))
        ax_tbl.set_title("Latest metrics", pad=14)

    plt.show()
    spark.stop()


if __name__ == "__main__":
    main()