import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, when, col

def norm_month(s: str) -> tuple[str, str]:
    s = s.strip()
    if "_" in s:
        return s, s.replace("_", "-")   # ('2015_01','2015-01')
    return s.replace("-", "_"), s       # ('2015_01','2015-01')

def main():
    # month as rgument, default: 2015_01
    month_arg = sys.argv[1] if len(sys.argv) > 1 else "2015_01"
    month_u, _ = norm_month(month_arg)

    in_path  = f"data/fact/{month_u}"
    out_path = f"data/gold/{month_u}"

    if not os.path.exists(in_path):
        raise FileNotFoundError(f"[ERR] Input non trovato: {in_path}")

    spark = SparkSession.builder.appName(f"Build Features {month_u}").getOrCreate()

    # read the fact (zone_id, ts_hour, pickups)
    df = spark.read.parquet(in_path)

    # Feature base
    df = df.withColumn("hour", hour("ts_hour")) \
           .withColumn("dow", dayofweek("ts_hour")) \
           .withColumn("is_weekend", when(col("dow").isin(1, 7), 1).otherwise(0))

    # write gold level
    df.write.mode("overwrite").parquet(out_path)
    print(f"[OK] Gold scritto in: {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
