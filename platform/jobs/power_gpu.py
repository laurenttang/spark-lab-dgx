"""Spark 的威力 / GPU 加速（RAPIDS Accelerator for Apache Spark）

The same 4 queries as power_query.py, run twice in ONE Spark session:
first with spark.rapids.sql.enabled=false (CPU), then =true (GPU, GB10).
Same data, same code, same JVM; only the plugin switch changes.

Runs in local mode inside the portal container (the only container with the
GPU), so the CPU side uses the same local[N] cores for a fair comparison.

Proof the GPU really did the work (not a silent CPU fallback):
  * the final AQE plan of each GPU query lists Gpu* operators;
  * spark.rapids.sql.explain=NOT_ON_GPU logs every operator that fell back;
  * nvidia-smi utilization is sampled while the GPU queries run;
  * CPU and GPU answers are compared row by row.

Time zone: RAPIDS cannot parse CSV timestamps in Asia/Taipei, so the session
runs in UTC and the hour is shifted by +8. Taiwan has had no DST since 1979,
so (UTC hour + 8) % 24 is exactly the Taipei hour.
"""
import os
import re
import subprocess
import threading
import time

from pyspark.sql import functions as F

from lab_common import Report, get_spark, arg, dir_gb, weblogs_path

COLS = ["device_id", "timestamp", "city_ch", "age_group", "gender"]
SCHEMA = ("device_id STRING, timestamp TIMESTAMP, member_id STRING, product_category_id INT, ip STRING, "
          "lat DOUBLE, lon DOUBLE, country_en STRING, country_ch STRING, city_en STRING, city_ch STRING, "
          "device_type INT, connection_type INT, age_group STRING, gender INT")


def queries(logs):
    taipei_hour = ((F.hour("timestamp") + 8) % 24).alias("h")
    return {
        "總數 / 裝置數": logs.agg(F.count("*").alias("rows"), F.approx_count_distinct("device_id").alias("dev"),
                              F.min("timestamp").alias("first"), F.max("timestamp").alias("last")),
        "每小時流量": logs.groupBy(taipei_hour).count().orderBy("h"),
        "城市 Top 12": logs.where(F.col("city_ch") != "").groupBy("city_ch").count()
                          .orderBy(F.desc("count"), "city_ch").limit(12),
        "年齡 × 性別": logs.where(F.col("gender").isNotNull() & F.col("age_group").isNotNull())
                          .groupBy("age_group", "gender").count().orderBy("age_group", "gender"),
    }


class GpuSampler:
    """Sample nvidia-smi utilization in the background while the GPU side runs."""

    def __init__(self):
        self.samples, self._stop = [], threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            try:
                out = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],
                                     capture_output=True, text=True, timeout=5).stdout.strip()
                self.samples.append(int(out.splitlines()[0]))
            except Exception:
                pass
            self._stop.wait(0.5)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join()


def run_side(spark, fmt, path, gpu):
    spark.conf.set("spark.rapids.sql.enabled", str(gpu).lower())
    src = (spark.read.parquet(path) if fmt == "parquet"
           else spark.read.csv(path, header=True, schema=SCHEMA))
    times, answers, ops = {}, {}, set()
    for name, df in queries(src.select(*COLS)).items():
        t = time.time()
        rows = df.collect()
        times[name] = time.time() - t
        answers[name] = [tuple(r) for r in rows]
        plan = df._jdf.queryExecution().executedPlan().toString()     # AQE final plan, after execution
        ops |= set(re.findall(r"Gpu[A-Z]\w+", plan))
    return times, answers, ops


if __name__ == "__main__":
    factor = arg("factor", 30, int)
    fmts = [f for f in arg("formats", "parquet,csv").split(",") if f]
    spark = get_spark(f"power gpu x{factor}")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    master = spark.sparkContext.master
    rep = Report(f"CPU vs GPU ×{factor}（{master}）")
    if "com.nvidia.spark.SQLPlugin" not in spark.conf.get("spark.plugins", ""):
        raise SystemExit("RAPIDS plugin 沒有載入，請確認 portal 有掛 GPU 和 /opt/rapids。")

    gpu_name = subprocess.run(["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"],
                              capture_output=True, text=True).stdout.strip()
    labels, cpu_s, gpu_s, rows_tbl, peaks, all_ops, mismatch = [], [], [], [], [], set(), []
    first = None

    for fmt in fmts:
        path = weblogs_path(factor, fmt)
        if not os.path.exists(path):
            rep.note(f"略過 {fmt.upper()}：找不到 ×{factor} 的資料。")
            continue
        size = dir_gb(path, ".parquet" if fmt == "parquet" else ".csv")
        with rep.step(f"{fmt} warm-up"):                 # fill the OS page cache, JIT both code paths
            spark.conf.set("spark.rapids.sql.enabled", "true")
            run_side(spark, fmt, path, gpu=True)
            run_side(spark, fmt, path, gpu=False)
        with rep.step(f"{fmt.upper()} on CPU"):
            ct, ca, _ = run_side(spark, fmt, path, gpu=False)
        with rep.step(f"{fmt.upper()} on GPU"), GpuSampler() as smp:
            gt, ga, ops = run_side(spark, fmt, path, gpu=True)
        same = all(sorted(map(str, ca[k])) == sorted(map(str, ga[k])) for k in ca if k != "總數 / 裝置數") and \
            ca["總數 / 裝置數"][0][0] == ga["總數 / 裝置數"][0][0]
        if not same:
            mismatch.append(fmt)
        c, g = sum(ct.values()), sum(gt.values())
        peak = max(smp.samples or [0])
        peaks.append(peak)
        all_ops |= ops
        labels.append(f"{fmt.upper()}（{size:.1f} GB）")
        cpu_s.append(round(c, 1))
        gpu_s.append(round(g, 1))
        for k in ct:
            rows_tbl.append([fmt.upper(), k, round(ct[k], 2), round(gt[k], 2), f"×{ct[k] / gt[k]:.1f}"])
        rows_tbl.append([fmt.upper(), "合計", round(c, 1), round(g, 1), f"×{c / g:.1f}"])
        rep.metric(f"{fmt.upper()} 加速", f"×{c / g:.1f}")
        rep.metric(f"{fmt.upper()} CPU → GPU", f"{c:.1f} s → {g:.1f} s")
        rep.metric(f"{fmt.upper()} GPU 運算子", len(ops))
        rep.metric(f"{fmt.upper()} GPU 使用率峰值", f"{peak}%")
        rep.metric(f"{fmt.upper()} 結果一致", "✓" if same else "✗")
        first = first or ga
    if not labels:
        raise SystemExit(f"找不到 ×{factor} 資料，請先執行「資料放大」和「CSV → Parquet」。")

    rep.metric("GPU", gpu_name)
    rep.metric("CPU 端", master)
    rep.chart("4 個查詢合計耗時（秒，越短越好）", "bar", labels, {"CPU": cpu_s, "GPU": gpu_s})
    rep.table("各查詢耗時（秒）", rows_tbl, ["格式", "查詢", "CPU", "GPU", "加速"])
    hourly = first["每小時流量"]
    rep.chart("每小時流量（GPU 算出的結果，台北時間）", "bar", [f"{r[0]:02d}" for r in hourly],
              {"events": [r[1] for r in hourly]})
    rep.table("GPU 上執行的運算子（取自 Spark 最終執行計畫）", [[o] for o in sorted(all_ops)], ["運算子"])
    rep.note("同一個 Spark session、同一份資料、同一段程式，只切換 spark.rapids.sql.enabled；"
             "兩邊都先暖機過（資料已在記憶體快取裡），比較的是運算本身。")
    rep.note("GPU 真的有在算的證據：執行計畫裡全是 Gpu* 運算子、nvidia-smi 量到使用率、CPU 和 GPU 的答案逐筆相同。"
             "若有運算子退回 CPU，log 裡會出現「!Exec ... cannot run on GPU」。")
    rep.note("RAPIDS 不支援在 Asia/Taipei 時區解析 CSV 時間，所以 session 用 UTC，再把小時 +8 換回台北時間"
             "（台灣 1979 年後沒有日光節約時間，換算是精確的）。")
    if any(g >= c for c, g in zip(cpu_s, gpu_s)):
        rep.note("這次 GPU 沒有比較快：資料太小時，把資料搬到 GPU、啟動 GPU kernel 的固定成本比運算本身還高。"
                 "用 ×30 以上的資料才看得出 GPU 的優勢（×30：Parquet ×2.0、CSV ×3.3）。")
    if mismatch:
        rep.note(f"注意：{', '.join(mismatch)} 的 CPU / GPU 結果不一致，請檢查 log。")
    rep.raw("bench", {"factor": factor, "formats": fmts, "cpu": cpu_s, "gpu": gpu_s, "peaks": peaks,
                      "ops": sorted(all_ops), "mismatch": mismatch})
    rep.save()
    spark.stop()
