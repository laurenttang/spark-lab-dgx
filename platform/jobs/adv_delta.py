"""進階主題 / Delta Lake：在 Parquet 上加交易紀錄

Delta Lake = Parquet data files + a transaction log (`_delta_log/*.json`). The log is what turns a
folder of files into a table with ACID commits, versions, MERGE / UPDATE / DELETE and per-file
statistics. This job walks through the parts that matter in practice:

1. Convert the amplified web logs (Parquet) into a Delta table, and look inside `_delta_log`.
2. Build a small `devices` table (one row per device) and MERGE a day of changes into it:
   update existing devices, insert new ones, delete devices that asked to be forgotten.
3. Make a mistake on purpose (UPDATE without WHERE), see it with time travel, undo it with RESTORE.
4. Data skipping: count how many files a one-day query has to open, before and after
   OPTIMIZE ... ZORDER BY (timestamp), using the min/max statistics stored in the log.

Needs the Delta jars (`--packages io.delta:delta-spark_4.1_2.13:4.4.1`, cached in /work/ivy) and the
Delta SQL extension / catalog; the portal adds both for this job.
"""
import datetime as dt
import glob
import json
import os
import shutil

from pyspark.sql import functions as F

from lab_common import WORK, Report, arg, dir_gb, get_spark, weblogs_path

ROOT = f"{WORK}/delta"
DAY = "2019-05-18"                                   # the one-day query used for data skipping


def log_files(path):
    return sorted(glob.glob(f"{path}/_delta_log/*.json"))


def active_files(path):
    """Replay the JSON commits: the table's current files with their min/max stats."""
    files = {}
    for f in log_files(path):
        for line in open(f):
            a = json.loads(line)
            if "add" in a:
                files[a["add"]["path"]] = dict(json.loads(a["add"].get("stats") or "{}"), size=a["add"]["size"])
            elif "remove" in a:
                files.pop(a["remove"]["path"], None)
    return files


def ts(v):
    return dt.datetime.fromisoformat(v.replace("Z", "+00:00"))


def files_for_day(files, day):
    """Files whose [min, max] timestamp overlaps the day (Taipei time): what Delta must open."""
    lo = dt.datetime.fromisoformat(f"{day}T00:00:00+08:00")
    hi = lo + dt.timedelta(days=1)
    n, size = 0, 0
    for st in files.values():
        mn, mx = st.get("minValues", {}).get("timestamp"), st.get("maxValues", {}).get("timestamp")
        if mn is None or mx is None or (ts(mn) < hi and ts(mx) >= lo):
            n, size = n + 1, size + st["size"]
    return n, size


def day_query(spark, path):
    return spark.read.format("delta").load(path) \
        .where((F.col("timestamp") >= F.lit(f"{DAY} 00:00:00").cast("timestamp")) &
               (F.col("timestamp") < F.lit(f"{DAY} 00:00:00").cast("timestamp") + F.expr("INTERVAL 1 DAY"))) \
        .count()


if __name__ == "__main__":
    factor = arg("factor", 30, int)
    optimize = arg("optimize", "yes") == "yes"
    src, fmt = weblogs_path(factor, "parquet"), "parquet"
    if not os.path.exists(src):
        src, fmt = weblogs_path(factor, "csv"), "csv"
    if not os.path.exists(src):
        raise SystemExit(f"找不到 ×{factor} 的資料，請先執行「資料放大」（或倍數填 1 用原始資料）。")

    spark = get_spark(f"advanced delta lake x{factor}")
    rep = Report(f"Delta Lake ×{factor}")
    fact, dev = f"{ROOT}/weblogs_x{factor}", f"{ROOT}/devices_x{factor}"
    shutil.rmtree(fact, ignore_errors=True)            # every run starts from version 0
    shutil.rmtree(dev, ignore_errors=True)

    # ---------------- 1. Parquet -> Delta ----------------
    with rep.step(f"① {fmt.upper() if fmt == 'csv' else 'Parquet'} → Delta（寫入資料 + 交易紀錄）"):
        if fmt == "parquet":
            df = spark.read.parquet(src)
        else:
            from power_to_parquet import SCHEMA
            df = spark.read.csv(src, header=True, schema=SCHEMA)
        df.write.format("delta").save(fact)
    rows = spark.read.format("delta").load(fact).count()
    files0 = active_files(fact)
    rep.metric("資料筆數", rows)
    rep.metric("Delta 資料檔", len(files0))
    rep.metric("Delta 大小", round(dir_gb(fact, ".parquet"), 2), " GB")
    rep.metric("交易紀錄大小", round(sum(os.path.getsize(f) for f in log_files(fact)) / 1024, 1), " KB")
    first = next(iter(files0.values()))
    sample = {"numRecords": first.get("numRecords"),
              "minValues": {k: first["minValues"].get(k) for k in ("timestamp", "city_ch", "age_group")},
              "maxValues": {k: first["maxValues"].get(k) for k in ("timestamp", "city_ch", "age_group")}}
    rep.note("_delta_log/00000000000000000000.json 是第 0 版的 commit：每個資料檔一筆 add 動作，"
             "附上這個檔的筆數和每欄 min / max。例如第一個檔：" + json.dumps(sample, ensure_ascii=False))

    # ---------------- 2. devices table + MERGE ----------------
    with rep.step("② 建 devices 表（每台裝置一列）"):
        spark.read.format("delta").load(fact).groupBy("device_id").agg(
            F.count("*").alias("events"), F.max("timestamp").alias("last_seen"),
            F.first("gender").alias("gender"), F.first("age_group").alias("age_group"),
            F.first("city_ch").alias("city")).write.format("delta").save(dev)
    devices = spark.read.format("delta").load(dev)
    n_dev = devices.count()
    probe = devices.orderBy(F.desc("events")).first().device_id      # follow one device through the versions

    with rep.step("③ MERGE：更新 / 新增 / 刪除"):
        ids = devices.select("device_id")
        upd_ids = ids.sample(fraction=0.05, seed=1)
        upd = upd_ids.withColumn("forget", F.lit(False))
        forget = ids.where(F.col("device_id") != probe).subtract(upd_ids).limit(100).withColumn("forget", F.lit(True))
        new = spark.range(500).select(F.concat(F.lit("new-device-"), F.col("id")).alias("device_id"),
                                      F.lit(False).alias("forget"))
        batch = upd.unionByName(forget).unionByName(new).union(
            spark.createDataFrame([(probe, False)], "device_id STRING, forget BOOLEAN")).dropDuplicates(["device_id"])
        batch = batch.withColumn("new_events", (F.rand(7) * 20 + 1).cast("int")) \
                     .withColumn("seen", F.lit("2019-07-01 12:00:00").cast("timestamp"))
        batch.createOrReplaceTempView("today")
        spark.sql(f"""
            MERGE INTO delta.`{dev}` t USING today s ON t.device_id = s.device_id
            WHEN MATCHED AND s.forget THEN DELETE
            WHEN MATCHED THEN UPDATE SET t.events = t.events + s.new_events, t.last_seen = s.seen
            WHEN NOT MATCHED AND NOT s.forget THEN
              INSERT (device_id, events, last_seen, gender, age_group, city)
              VALUES (s.device_id, s.new_events, s.seen, NULL, NULL, NULL)""")

    # ---------------- 3. mistake -> time travel -> restore ----------------
    with rep.step("④ 手滑：UPDATE 忘了 WHERE"):
        spark.sql(f"UPDATE delta.`{dev}` SET gender = NULL")
    nulls_bad = spark.read.format("delta").load(dev).where("gender IS NULL").count()
    with rep.step("⑤ RESTORE 回第 1 版"):
        spark.sql(f"RESTORE TABLE delta.`{dev}` TO VERSION AS OF 1")
    nulls_ok = spark.read.format("delta").load(dev).where("gender IS NULL").count()

    tt = []
    for v, what in [(0, "建表"), (1, "MERGE 之後"), (2, "手滑 UPDATE 之後"), (3, "RESTORE 之後")]:
        d = spark.read.format("delta").option("versionAsOf", v).load(dev)
        p = d.where(F.col("device_id") == probe).first()
        tt.append([v, what, d.count(), d.where("gender IS NULL").count(),
                   p.events if p else None, str(p.last_seen) if p else None, p.gender if p else None])
    rep.table(f"時間旅行：同一張表的每個版本（追蹤裝置 {probe[:13]}…）", tt,
              columns=["版本", "動作", "裝置數", "gender 為空", "這台的 events", "這台的 last_seen", "這台的 gender"])

    hist = spark.sql(f"DESCRIBE HISTORY delta.`{dev}`").orderBy("version").collect()
    keep = ["numTargetRowsUpdated", "numTargetRowsInserted", "numTargetRowsDeleted", "numUpdatedRows",
            "numTargetFilesAdded", "numTargetFilesRemoved", "numAddedFiles", "numRemovedFiles",
            "numRestoredFiles", "numOutputRows"]
    rep.table("DESCRIBE HISTORY（devices 表）", [
        [h.version, h.operation, ", ".join(f"{k}={h.operationMetrics[k]}" for k in keep
                                           if h.operationMetrics and k in h.operationMetrics)]
        for h in hist], columns=["版本", "動作", "operationMetrics"])
    m = {h.operation: h.operationMetrics for h in hist}.get("MERGE", {})
    rep.metric("MERGE 更新", int(m.get("numTargetRowsUpdated", 0)))
    rep.metric("MERGE 新增", int(m.get("numTargetRowsInserted", 0)))
    rep.metric("MERGE 刪除", int(m.get("numTargetRowsDeleted", 0)))
    rep.metric("手滑後 gender 為空", nulls_bad)
    rep.metric("RESTORE 後 gender 為空（MERGE 新增的裝置本來就沒有）", nulls_ok)
    rep.note(f"devices 表有 {n_dev:,} 台裝置。MERGE 一次完成三件事：5% 的裝置更新 events / last_seen、"
             f"新增 500 台、刪除 100 台要求被遺忘的裝置；整個 MERGE 是一個 commit，要嘛全部成功，要嘛完全沒發生。")
    rep.note("Delta 不改舊檔（copy-on-write）：MERGE / UPDATE 會寫出新的檔，在 log 裡 remove 舊檔、add 新檔。"
             "舊檔還在磁碟上，所以能用 versionAsOf 讀舊版本，也能 RESTORE；VACUUM 之後超過保留期的舊檔才會真的刪掉。")

    # ---------------- 4. data skipping ----------------
    def gb(b):
        return round(b / 1024 ** 3, 3)

    skip = []
    n, size = files_for_day(files0, DAY)
    skip.append(["寫入後（未排序）", len(files0), n, gb(size), day_query(spark, fact)])
    if optimize:
        with rep.step("⑥ OPTIMIZE ZORDER BY (timestamp)"):
            # target ~32 MB files (default 1 GB would leave this 1.8 GB table in 2 files: nothing to skip)
            spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(32 * 1024 * 1024))
            spark.sql(f"OPTIMIZE delta.`{fact}` ZORDER BY (`timestamp`)")   # timestamp is a keyword
        files1 = active_files(fact)
        n, size = files_for_day(files1, DAY)
        skip.append(["OPTIMIZE ZORDER 後", len(files1), n, gb(size), day_query(spark, fact)])
    rep.table(f"Data skipping：查 {DAY} 一天要開幾個檔", skip,
              columns=["狀態", "資料檔數", "要開的檔（min/max 和這天重疊）", "這些檔的大小 GB", "查到筆數（兩次一樣）"])
    rep.chart(f"查 {DAY} 一天：需要讀的檔 vs 全部的檔", "bar", [s[0] for s in skip],
              {"全部資料檔": [s[1] for s in skip], "需要讀": [s[2] for s in skip]})
    rep.note("Data skipping：查詢前先看 log 裡每個檔的 timestamp min / max，和查詢範圍不重疊的檔直接跳過，連開都不用開。"
             "剛寫入時每個檔都混了整個月的資料，所以一個都跳不掉；OPTIMIZE ZORDER BY 把資料依時間重排，"
             "每個檔只涵蓋一小段時間，大部分檔就能跳過。"
             "這裡比的是要讀的檔數和資料量；在這個規模下查詢秒數主要是排程等固定成本，看不太出差別，資料越大越明顯。")
    if optimize:
        # ---------------- 5. VACUUM: the old files are what time travel reads ----------------
        before = dir_gb(fact, ".parquet")
        with rep.step("⑦ VACUUM（刪掉已經不在目前版本裡的舊檔）"):
            # default retention is 7 days; 0 hours only for the demo (it breaks readers on old versions)
            spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            spark.sql(f"VACUUM delta.`{fact}` RETAIN 0 HOURS")
        after = dir_gb(fact, ".parquet")
        old = spark.read.format("delta").option("versionAsOf", 0).load(fact)

        def attempt(f):
            try:
                f()
                return "可以"
            except Exception as e:                      # FAILED_READ_FILE.FILE_NOT_EXIST
                return "失敗：" + ("FILE_NOT_EXIST，舊檔已刪" if "FILE_NOT_EXIST" in str(e) else type(e).__name__)
        rep.table("VACUUM 前後（weblogs 表）", [
            ["磁碟上的資料檔", f"{before:.2f} GB", f"{after:.2f} GB"],
            ["第 0 版 count()（只看 log 裡的筆數統計，不開資料檔）", "可以", attempt(lambda: old.count())],
            ["第 0 版實際讀資料（sum(length(device_id))）", "可以",
             attempt(lambda: old.select(F.sum(F.length("device_id"))).first())]],
            columns=["", "VACUUM 前", "VACUUM 後"])
        rep.note("OPTIMIZE 寫了一份重排過的新檔，舊檔仍留著給時間旅行用，所以磁碟上暫時是兩份。VACUUM 刪掉超過保留期、"
                 "已經不屬於目前版本的檔：省下空間，但也就回不到那些舊版本了。預設保留 7 天，這裡為了示範設成 0。"
                 "注意 count() 還是會成功：Delta 直接用 log 裡每個檔的 numRecords 回答，不開資料檔，"
                 "所以「count 得出來」不代表舊版本還在。")
    rep.metric("Delta log 版本數（devices）", len(log_files(dev)))
    rep.save()
    spark.stop()
