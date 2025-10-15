import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, lag, avg, hour, dayofweek, when

def norm_month(s: str) -> tuple[str, str]:
    s = s.strip()
    if "_" in s:
        return s, s.replace("_", "-")   # ('2015_01','2015-01')
    return s.replace("-", "_"), s       # ('2015_01','2015-01')

def main():
    # month as argument, default: 2015_01
    month_arg = sys.argv[1] if len(sys.argv) > 1 else "2015_01"
    month_u, _ = norm_month(month_arg)

    in_path  = f"data/fact/{month_u}"
    out_path = f"data/gold_lag/{month_u}"

    if not os.path.exists(in_path):
        raise FileNotFoundError(f"[ERR] Input non trovato: {in_path}")

    spark = SparkSession.builder.appName(f"Build Lag Features {month_u}").getOrCreate()

    # fact: zone_id, ts_hour (timestamp), pickups
    df = spark.read.parquet(in_path)

    # Calendar features
    df = df.withColumn("hour", hour("ts_hour")) \
           .withColumn("dow", dayofweek("ts_hour")) \
           .withColumn("is_weekend", when(col("dow").isin(1, 7), 1).otherwise(0))

    # temporal window for zone time ordered
    w = Window.partitionBy("zone_id").orderBy("ts_hour")

    # classic Lag
    df = df.withColumn("lag1",   lag("pickups", 1).over(w)) \
           .withColumn("lag24",  lag("pickups", 24).over(w)) \
           .withColumn("lag168", lag("pickups", 168).over(w))

    # Rolling mean on window
    # NB: rowsBetween count rows, not real time. 
    w24  = w.rowsBetween(-24, -1)
    w168 = w.rowsBetween(-168, -1)
    df = df.withColumn("roll24_mean",  avg("pickups").over(w24)) \
           .withColumn("roll168_mean", avg("pickups").over(w168))

    # write GOLD_LAG
    df.write.mode("overwrite").parquet(out_path)
    print(f"[OK] Gold_lag scritto in: {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
