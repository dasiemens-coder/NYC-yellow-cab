import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, count, to_timestamp
from pyspark.sql.types import TimestampType, StringType

def norm_month(s: str) -> tuple[str, str]:
    s = s.strip()
    if "_" in s:
        return s, s.replace("_", "-")   # ('2015_01','2015-01')
    return s.replace("-", "_"), s       # ('2015_01','2015-01')

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def main():
    # month as argument: 2015_01 or 2015-01
    month_arg = sys.argv[1] if len(sys.argv) > 1 else "2015_01"
    month_u, _ = norm_month(month_arg)

    in_path  = f"data/silver/{month_u}"
    out_path = f"data/fact/{month_u}"

    if not os.path.exists(in_path):
        raise FileNotFoundError(f"[ERR] Input non trovato: {in_path}")

    spark = SparkSession.builder.appName(f"Build Fact Demand {month_u}").getOrCreate()

    # read silver
    df = spark.read.parquet(in_path)
    
    # Find col pickup timestamp and zone
    pickup_candidates = [
        "tpep_pickup_datetime", "pickup_datetime", "pickup_datetime_ts"
    ]
    zone_candidates = [
        "PULocationID", "pu_location_id", "PUlocationID", "pickup_zone_id"
    ]

    pickup_col = pick_col(df, pickup_candidates)
    zone_col   = pick_col(df, zone_candidates)

    if pickup_col is None:
        raise ValueError(f"[ERR] Colonna timestamp pickup non trovata. Cercate: {pickup_candidates}")
    if zone_col is None:
        raise ValueError(f"[ERR] Colonna zona non trovata. Cercate: {zone_candidates}")

    dtype = dict(df.dtypes)[pickup_col]
    if dtype in ("string",) or isinstance(df.schema[pickup_col].dataType, StringType):
        df = df.withColumn(pickup_col, to_timestamp(col(pickup_col)))

    #round to the hours and aggregate
    df = df.withColumn("ts_hour", date_trunc("hour", col(pickup_col)))

    fact = df.groupBy(col(zone_col).alias("zone_id"), col("ts_hour")) \
             .agg(count("*").alias("pickups"))


    fact.write.mode("overwrite").parquet(out_path)
    print(f"[OK] Fact scritto in: {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
