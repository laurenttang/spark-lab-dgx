"""Spark 的威力 / CSV → Parquet（課程 Big Data File Formats 單元）

Convert the amplified CSV logs into Parquet (columnar, snappy-compressed, with a
typed schema and per-column min/max statistics). power_query.py --format parquet
then reads only the 5 columns it needs instead of parsing every CSV line.

Output: /work/bigdata/parquet_x{factor}/
"""
import os
import shutil

from lab_common import Report, get_spark, arg, dir_gb, weblogs_path

SCHEMA = ("device_id STRING, timestamp TIMESTAMP, member_id STRING, product_category_id INT, ip STRING, "
          "lat DOUBLE, lon DOUBLE, country_en STRING, country_ch STRING, city_en STRING, city_ch STRING, "
          "device_type INT, connection_type INT, age_group STRING, gender INT")


if __name__ == "__main__":
    factor = arg("factor", 30, int)
    src, out = weblogs_path(factor), weblogs_path(factor, "parquet")
    if not os.path.exists(src):
        raise SystemExit(f"找不到 ×{factor} 的 CSV，請先執行「資料放大」。")
    spark = get_spark(f"power csv->parquet x{factor}")
    rep = Report(f"CSV → Parquet ×{factor}")

    with rep.step("read CSV + write Parquet (snappy)"):
        df = spark.read.csv(src, header=True, schema=SCHEMA)
        shutil.rmtree(out, ignore_errors=True)
        df.write.mode("overwrite").option("compression", "snappy").parquet(out)
    with rep.step("count rows from Parquet footer"):
        rows = spark.read.parquet(out).count()      # answered from file metadata, no data scan

    csv_gb, pq_gb = dir_gb(src, ".csv"), dir_gb(out, ".parquet")
    secs = rep._steps[0]["seconds"]
    rep.metric("筆數", rows)
    rep.metric("CSV 大小", f"{csv_gb:.2f} GB")
    rep.metric("Parquet 大小", f"{pq_gb:.2f} GB")
    rep.metric("壓縮比", f"{csv_gb / pq_gb:.1f} : 1")
    rep.metric("轉檔耗時", f"{secs:.1f} s")
    rep.metric("轉檔速度", f"{csv_gb * 1024 / secs:.0f} MB/s（讀 CSV）")
    rep.chart("磁碟大小（GB）", "bar", ["CSV", "Parquet"], {"GB": [round(csv_gb, 2), round(pq_gb, 2)]})
    rep.note("Parquet 是欄式儲存：同一欄的值放在一起，壓縮率高；檔尾記錄 schema、筆數和每欄的 min/max，"
             "所以 count() 幾乎不用讀資料，查詢也只需要讀用到的欄位。")
    rep.note(f"輸出位置：{out}（主機上是 ~/Spark/work/bigdata/parquet_x{factor}）。")
    rep.raw("dataset", {"factor": factor, "path": out, "rows": rows, "csv_gb": round(csv_gb, 3),
                        "parquet_gb": round(pq_gb, 3), "seconds": secs})
    rep.save()
    spark.stop()
