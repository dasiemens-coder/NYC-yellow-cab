import sys
import os
from pyspark.sql import SparkSession

def norm_month(s: str) -> tuple[str, str]:
    """Accetta '2015_01' o '2015-01' e ritorna ('2015_01','2015-01')."""
    s = s.strip()
    if "_" in s:
        return s, s.replace("_", "-")
    return s.replace("-", "_"), s

def main():
    # month parameter (default: 2015_01)
    month_arg = sys.argv[1] if len(sys.argv) > 1 else "2015_01"
    month_u, month_d = norm_month(month_arg)

    # Path input (raw) and output (silver)
    in_path  = f"data/raw/yellow_tripdata_{month_d}.parquet"
    out_path = f"data/silver/{month_u}"

    if not os.path.exists(in_path):
        raise FileNotFoundError(f"[ERR] Non trovato il file raw: {in_path}")

    spark = SparkSession.builder.appName(f"NYC Taxi Ingest {month_u}").getOrCreate()

    # red Parquet raw
    df = spark.read.parquet(in_path)

    print("Schema:")
    df.printSchema()
    print("Prime 5 righe:")
    df.show(5, truncate=False)

    # write in silver (format: Spark-friendly)
    df.write.mode("overwrite").parquet(out_path)
    print(f"[OK] Scritto silver in: {out_path}")

    spark.stop()

if __name__ == "__main__":
    main()
