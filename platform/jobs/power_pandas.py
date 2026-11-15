"""Spark 的威力 / 對照組：單機 pandas

Same analysis as power_query.py, but with pandas in ONE process on ONE core.
pandas must load everything into memory, so for big data it measures a
sample that safely fits and extrapolates the full run time and RAM needed.
(No Spark here: this is what you would do without a cluster.)
"""
import glob
import os
import resource
import time

import pandas as pd

from lab_common import Report, arg, weblogs_path

COLS = ["device_id", "timestamp", "city_ch", "age_group", "gender"]
LIMIT_GB = 3.0          # max CSV we let pandas actually load inside the 8 GB portal container
LIMIT_ROWS = 16_000_000  # same idea for Parquet (≈ 3 GB of CSV), counted from the file footers

if __name__ == "__main__":
    factor = arg("factor", 1, int)
    fmt = arg("format", "csv").lower()
    path = weblogs_path(factor, fmt)
    files = sorted(glob.glob(os.path.join(path, "*.parquet" if fmt == "parquet" else "*.csv")))
    if not files:
        raise SystemExit(f"找不到 ×{factor} 的 {fmt} 資料，請先執行「資料放大」" +
                         ("和「CSV → Parquet」。" if fmt == "parquet" else "。"))
    rep = Report(f"pandas 單機對照 ×{factor} {fmt.upper()}")
    if fmt == "parquet":
        import pyarrow.parquet as pq
        weight = {f: pq.ParquetFile(f).metadata.num_rows for f in files}   # share of the work = rows
        limit = LIMIT_ROWS
    else:
        weight = {f: os.path.getsize(f) for f in files}
        limit = LIMIT_GB * 1024 ** 3
    total_gb = sum(os.path.getsize(f) for f in files) / 1024 ** 3

    picked, w = [], 0
    for f in files:
        if picked and w + weight[f] > limit:
            break
        picked.append(f)
        w += weight[f]
    gb = sum(os.path.getsize(f) for f in picked) / 1024 ** 3
    scale = sum(weight.values()) / w

    t0 = time.time()
    with rep.step(f"pandas read {len(picked)}/{len(files)} files"):
        read = (lambda f: pd.read_parquet(f, columns=COLS)) if fmt == "parquet" else \
               (lambda f: pd.read_csv(f, usecols=COLS))
        df = pd.concat((read(f) for f in picked), ignore_index=True)
    with rep.step("pandas aggregate"):
        ts = df["timestamp"] if fmt == "parquet" else pd.to_datetime(df["timestamp"].str[:19])
        df["h"] = ts.dt.hour
        hourly = df.groupby("h").size()
        df.groupby("city_ch").size().nlargest(12)
        df.groupby(["age_group", "gender"]).size()
        devices = df["device_id"].nunique()
    secs = time.time() - t0
    peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 ** 2

    rep.metric("格式", fmt.upper())
    rep.metric("全部資料", f"{total_gb:.2f} GB")
    rep.metric("pandas 實際載入", f"{gb:.2f} GB ({len(df):,} 筆)")
    rep.metric("實測耗時", f"{secs:.1f} s")
    rep.metric("峰值記憶體", f"{peak_gb:.1f} GB")
    if scale > 1.01:
        rep.metric("推估全量耗時", f"{secs * scale:.0f} s")
        rep.metric("推估全量記憶體", f"{peak_gb * scale:.0f} GB")
        rep.note(f"資料有 {total_gb:.1f} GB，pandas 必須整份載入記憶體，估計要 {peak_gb * scale:.0f} GB RAM，"
                 f"超過容器上限，所以只實測前 {gb:.1f} GB，再等比例推估。Spark 則是分批串流處理，不受記憶體限制。")
    rep.metric("處理速度", f"{len(df) / secs / 1e6:.2f} M 筆/秒")
    rep.chart("每小時流量（pandas 結果）", "bar", [f"{h:02d}" for h in hourly.index], {"events": hourly.tolist()})
    rep.raw("bench", {"factor": factor, "engine": "pandas", "seconds": secs, "est_seconds": secs * scale,
                      "rows": len(df), "gb": round(gb, 3), "total_gb": round(total_gb, 3), "peak_gb": round(peak_gb, 2),
                      "scale": round(scale, 3), "format": fmt})
    rep.save()
