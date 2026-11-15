"""Spark 的威力 / 大量資料分析

One realistic analytics pass over the amplified web logs: full scan of every
CSV file, then several aggregations (hourly traffic, top cities, gender x
age, distinct devices). Run it with different core counts to see how Spark
scales out, and with --format parquet to see what a columnar file format buys
(only 5 of 15 columns are read, no text parsing).
"""
import glob
import os

from pyspark.sql import functions as F

from lab_common import Report, get_spark, arg, dir_gb, weblogs_path

COLS = ["device_id", "timestamp", "city_ch", "age_group", "gender"]


def parquet_column_gb(path, cols):
    """Compressed bytes of just these columns, from the Parquet footers (what Spark actually reads)."""
    import pyarrow.parquet as pq
    total = 0
    for f in glob.glob(os.path.join(path, "*.parquet")):
        md = pq.ParquetFile(f).metadata
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            total += sum(rg.column(c).total_compressed_size for c in range(rg.num_columns)
                         if rg.column(c).path_in_schema in cols)
    return total / 1024 ** 3


if __name__ == "__main__":
    factor = arg("factor", 1, int)
    fmt = arg("format", "csv").lower()
    path = weblogs_path(factor, fmt)
    if not os.path.exists(path):
        raise SystemExit(f"找不到 ×{factor} 的 {fmt} 資料，請先執行「資料放大」" +
                         ("和「CSV → Parquet」。" if fmt == "parquet" else "。"))

    spark = get_spark(f"power query x{factor} {fmt}")
    cores = int(spark.conf.get("spark.cores.max", "16"))
    rep = Report(f"大量資料分析 ×{factor} {fmt.upper()}（{cores} cores）")
    schema = ("device_id STRING, timestamp TIMESTAMP, member_id STRING, product_category_id INT, ip STRING, "
              "lat DOUBLE, lon DOUBLE, country_en STRING, country_ch STRING, city_en STRING, city_ch STRING, "
              "device_type INT, connection_type INT, age_group STRING, gender INT")

    with rep.step("scan + aggregate"):
        # each format gets its best strategy:
        #  CSV     -> parse once, cache the 5 needed columns in executor memory, run every query
        #             on the cache (otherwise each query would re-parse 6.7 GB of text)
        #  Parquet -> no cache: each query reads just its columns straight from the files, which is
        #             cheaper than building an in-memory copy (measured: 12.5 s vs 37 s at x30/16 cores)
        if fmt == "parquet":
            logs = spark.read.parquet(path).select(*COLS)
        else:
            logs = spark.read.csv(path, header=True, schema=schema).select(*COLS).cache()
        stats = logs.agg(F.count("*").alias("rows"),
                         F.approx_count_distinct("device_id").alias("devices"),
                         F.min("timestamp").alias("first"), F.max("timestamp").alias("last")).first()
        hourly = logs.groupBy(F.hour("timestamp").alias("h")).count().orderBy("h").collect()
        cities = logs.where(F.col("city_ch") != "").groupBy("city_ch").count() \
                     .orderBy(F.desc("count")).limit(12).collect()
        demo = logs.where(F.col("gender").isNotNull() & F.col("age_group").isNotNull()) \
                   .groupBy("age_group", "gender").count().orderBy("age_group", "gender").collect()

    secs = rep._steps[-1]["seconds"]
    size_gb = dir_gb(path, ".parquet" if fmt == "parquet" else ".csv")
    read_gb = parquet_column_gb(path, COLS) if fmt == "parquet" else size_gb
    rep.metric("資料筆數", stats.rows)
    rep.metric("格式", fmt.upper())
    rep.metric("資料大小", f"{size_gb:.2f} GB")
    if fmt == "parquet":
        rep.metric("實際讀取", f"{read_gb:.2f} GB（{len(COLS)}/15 欄）")
    rep.metric("使用核心", cores)
    rep.metric("分析耗時", f"{secs:.1f} s")
    rep.metric("處理速度", f"{stats.rows / secs / 1e6:.2f} M 筆/秒")
    rep.metric("吞吐量", f"{size_gb * 1024 / secs:.0f} MB/s（{fmt.upper()} 檔案）")
    if fmt == "parquet":
        rep.note("Parquet 是欄式格式：Spark 只讀這次用到的 5 欄（column pruning），而且欄位已有型別，"
                 "不必像 CSV 那樣逐行切字串、轉數字和時間。因為重讀很便宜，4 個查詢直接各自讀檔，"
                 "不必先把資料 cache 進記憶體；CSV 則要先解析一次並 cache，否則每個查詢都得重新解析。")
    rep.metric("不重複裝置（估計）", stats.devices)
    rep.chart("每小時流量", "bar", [f"{r.h:02d}" for r in hourly], {"events": [r["count"] for r in hourly]})
    rep.chart("城市 Top 12", "hbar", [r.city_ch for r in cities], {"events": [r["count"] for r in cities]})
    ages = sorted({r.age_group for r in demo})
    rep.chart("年齡層 × 性別", "bar", ages,
              {g: [next((r["count"] for r in demo if r.age_group == a and r.gender == gi), 0) for a in ages]
               for gi, g in [(0, "Female"), (1, "Male")]})
    rep.raw("bench", {"factor": factor, "cores": cores, "seconds": secs, "rows": stats.rows,
                      "gb": round(size_gb, 3), "read_gb": round(read_gb, 3), "format": fmt})
    rep.save()
    spark.stop()
