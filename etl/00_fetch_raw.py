# etl/00_fetch_raw.py
import os
import sys
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def norm_month(s: str) -> tuple[str, str]:
    s = s.strip()
    if "_" in s:
        return s, s.replace("_", "-")   # ('2015_01','2015-01')
    return s.replace("-", "_"), s       # ('2015_01','2015-01')

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def head_content_length(url: str) -> int | None:
    try:
        with urlopen(url) as r:
            return int(r.headers.get("Content-Length", "0"))
    except Exception:
        return None

def fetch_month(month_arg: str) -> None:
    m_u, m_d = norm_month(month_arg)
    fname = f"yellow_tripdata_{m_d}.parquet"
    url = f"{BASE}/{fname}"
    out_dir = "data/raw"
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, fname)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        print(f"[SKIP] Già presente: {out_path}")
        return

    print(f"[GET ] {url}")
    try:
        # opzionale: dimensione attesa
        size = head_content_length(url)
        urlretrieve(url, out_path)
        sz = os.path.getsize(out_path)
        if size and sz != size:
            raise RuntimeError(f"Dimensione inattesa: got {sz}, attesa {size}")
        if sz == 0:
            raise RuntimeError("File scaricato vuoto.")
        print(f"[OK  ] Salvato: {out_path} ({sz/1_048_576:.1f} MiB)")
    except (HTTPError, URLError) as e:
        if os.path.exists(out_path):
            os.remove(out_path)
        print(f"[ERR ] HTTP/URL error per {url}: {e}")
        sys.exit(1)
    except Exception as e:
        if os.path.exists(out_path):
            os.remove(out_path)
        print(f"[ERR ] {e}")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Uso: python etl/00_fetch_raw.py 2015_01 [2016_01 2016_02 2016_03 ...]")
        sys.exit(2)
    for arg in sys.argv[1:]:
        fetch_month(arg)

if __name__ == "__main__":
    main()

