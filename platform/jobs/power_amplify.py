"""Spark 的威力 / 資料放大

Multiply the real May-2019 e-commerce web logs (1.28M rows, ~300 MB) into a
much bigger synthetic dataset by cross-joining with spark.range(factor) and
jittering every copy (new device ids, shifted dates, nudged GPS points).
The distribution stays realistic; only the volume grows.

Output: /work/bigdata/weblogs_x{factor}/  (CSV, same layout as the original)
"""
import shutil
import subprocess

from pyspark.sql import functions as F

from lab_common import COURSE, WORK, Report, get_spark, arg

if __name__ == "__main__":
    factor = arg("factor", 30, int)
    spark = get_spark(f"power amplify x{factor}")
    rep = Report(f"資料放大 ×{factor}")
    out = f"{WORK}/bigdata/weblogs_x{factor}"

    with rep.step("read original 8 CSV files"):
        base = spark.read.csv(f"{COURSE}/spark_mllib_101/web_logs_analysis/data/", header=True, inferSchema=True)
        base_rows = base.count()

    with rep.step(f"cross join x{factor} + jitter + write CSV"):
        copies = spark.range(factor).withColumnRenamed("id", "copy")
        big = base.crossJoin(F.broadcast(copies)).select(
            F.when(F.col("copy") == 0, F.col("device_id"))
             .otherwise(F.concat(F.substring("device_id", 1, 30), F.format_string("%06d", "copy")))
             .alias("device_id"),
            (F.col("timestamp") + F.expr("make_interval(0, 0, 0, CAST(copy % 28 AS INT))")
             ).alias("timestamp"),
            "member_id", "product_category_id", "ip",
            (F.col("lat") + (F.rand(1) - 0.5) * 0.02 * F.least(F.col("copy"), F.lit(1))).alias("lat"),
            (F.col("lon") + (F.rand(2) - 0.5) * 0.02 * F.least(F.col("copy"), F.lit(1))).alias("lon"),
            "country_en", "country_ch", "city_en", "city_ch", "device_type", "connection_type",
            "age_group", "gender")
        shutil.rmtree(out, ignore_errors=True)
        big.repartition(max(16, factor)).write.mode("overwrite").option("header", True).csv(out)

    size = subprocess.run(["du", "-sb", out], capture_output=True, text=True).stdout.split()[0]
    gb = int(size) / 1024 ** 3
    rep.metric("放大倍數", f"×{factor}")
    rep.metric("原始筆數", base_rows)
    rep.metric("放大後筆數", base_rows * factor)
    rep.metric("資料大小", f"{gb:.1f} GB")
    rep.metric("寫入速度", f"{gb * 1024 / rep._steps[-1]['seconds']:.0f} MB/s")
    rep.note(f"輸出位置：{out}（主機上是 ~/Spark/work/bigdata/weblogs_x{factor}）。不需要時可在控制台刪除。")
    rep.raw("dataset", {"factor": factor, "path": out, "rows": base_rows * factor, "gb": round(gb, 2)})
    rep.save()
    spark.stop()
