"""進階主題 / Watermark 與遲到資料（Kafka + Structured Streaming）

A controlled experiment instead of an endless stream:

1. A producer thread writes `duration` seconds of events to a fresh Kafka topic (3 partitions).
   Each event carries its *event time*. Most are on time (0–2 s old when sent); `late_pct` %
   are deliberately late, in three groups: 3–8 s, 10–25 s and 30–50 s.
2. The same topic is read by several streaming queries at once, identical except for the
   watermark (e.g. 0 s / 10 s / 60 s). Each counts events per 10-second event-time window in
   **append** mode, so a window is written once, when the watermark passes its end, and never
   changes afterwards.
3. Because the producer knows every event it sent, the job compares each query's output with
   the true counts: what share of events made it in (completeness), how long after a window
   closed its result came out (latency), and how much state Spark had to keep (memory).

The trade-off it shows: a longer watermark waits for more late data (more complete), but
results come out later and more windows stay in state.
"""
import random
import shutil
import threading
import time
from collections import defaultdict

from kafka import KafkaProducer
from pyspark.sql import functions as F

from lab_common import WORK, Report, arg, get_spark

BOOT = "kafka:9092"
WINDOW = 10                                           # tumbling event-time window, seconds
BUCKETS = [("準時 0–2 s", 0, 2), ("稍晚 3–8 s", 3, 8), ("晚 10–25 s", 10, 25), ("很晚 30–50 s", 30, 50)]
FLUSH_MS = 3600 * 1000                                 # flush event: 1 h in the future


def clamp(v, lo, hi):
    return max(lo, min(v, hi))


def produce(topic, duration, rate, late_pct, truth, done):
    """Send events for `duration` s; record the true count per (window, bucket)."""
    producer = KafkaProducer(bootstrap_servers=BOOT, linger_ms=20)
    rnd = random.Random(42)
    t_end = time.time() + duration
    while time.time() < t_end:
        tick = time.time()
        for _ in range(rate):
            b = rnd.randrange(1, 4) if rnd.random() < late_pct / 100 else 0
            _, lo, hi = BUCKETS[b]
            ev_ms = int((time.time() - rnd.uniform(lo, hi)) * 1000)
            truth[(ev_ms // 1000 // WINDOW * WINDOW, b)] += 1
            producer.send(topic, f"{ev_ms},{b}".encode())
        producer.flush()
        time.sleep(max(0.0, 1 - (time.time() - tick)))
    # one event far in the future pushes every watermark past the real windows, so append mode
    # emits them; it lands in its own window, which the report ignores
    producer.send(topic, f"{int(time.time() * 1000) + FLUSH_MS},0".encode())
    producer.flush()
    producer.close()
    done.set()


def make_topic(topic):
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers=BOOT)
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        admin.close()
    except Exception as e:                            # broker auto-creates with num.partitions=3
        print("[note] create_topics:", e, flush=True)


def drop_topic(topic):
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=BOOT)
        admin.delete_topics([topic])
        admin.close()
    except Exception as e:                            # retention (24 h / 1 GB) removes the data anyway
        print("[note] delete_topics:", e, flush=True)


if __name__ == "__main__":
    duration = clamp(arg("duration", 60, int), 20, 300)
    rate = clamp(arg("rate", 200, int), 10, 5000)
    late_pct = clamp(arg("late_pct", 30, int), 0, 90)
    wms = sorted({clamp(int(x), 0, 300) for x in str(arg("watermarks", "0,10,60")).split(",")
                  if x.strip().isdigit()})[:4] or [0, 10, 60]

    run = str(int(time.time()))
    topic = f"watermark_demo_{run}"
    spark = get_spark("advanced watermark")
    spark.conf.set("spark.sql.shuffle.partitions", "4")                 # tiny state, fast micro-batches
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "2000")
    rep = Report(f"Watermark 實驗：{duration} 秒 × {rate} 筆/秒，遲到 {late_pct}%")

    make_topic(topic)
    raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT) \
        .option("subscribe", topic).option("startingOffsets", "earliest").load()
    arr = F.split(F.col("value").cast("string"), ",")
    events = raw.select((arr[0].cast("long") / 1000).cast("timestamp").alias("event_time"),
                        arr[1].cast("int").alias("bucket"))

    out = {wm: {} for wm in wms}                       # wm -> {(win_start, bucket): count}
    emitted = {wm: {} for wm in wms}                   # wm -> {win_start: wall time written}

    def sink(wm):
        def f(df, _batch_id):
            now = time.time()
            for r in df.collect():
                out[wm][(r.w, r.bucket)] = r["count"]
                emitted[wm].setdefault(r.w, now)
        return f

    queries = {}
    ckpts = []
    for wm in wms:
        ck = f"{WORK}/checkpoints/watermark_{run}_{wm}"
        ckpts.append(ck)
        agg = events.withWatermark("event_time", f"{wm} seconds") \
            .groupBy(F.window("event_time", f"{WINDOW} seconds"), "bucket").count() \
            .select(F.col("window.start").cast("long").alias("w"), "bucket", "count")
        queries[wm] = agg.writeStream.outputMode("append").trigger(processingTime="2 seconds") \
            .option("checkpointLocation", ck).queryName(f"wm_{wm}s").foreachBatch(sink(wm)).start()

    truth = defaultdict(int)
    done = threading.Event()
    try:
        with rep.step(f"送出 {duration} 秒事件到 Kafka，{len(wms)} 個查詢同時處理"):
            t0 = time.time()
            threading.Thread(target=produce, args=(topic, duration, rate, late_pct, truth, done),
                             daemon=True).start()
            done.wait(duration + 60)
            t_prod_end = time.time()
        last_win = int(t_prod_end) // WINDOW * WINDOW
        with rep.step("等所有視窗輸出（flush 事件推進 watermark）"):
            deadline = time.time() + 90
            while time.time() < deadline and not all(
                    any(w >= last_win - WINDOW for w in emitted[wm]) for wm in wms):
                time.sleep(1)
            time.sleep(4)
    finally:
        progress = {wm: q.recentProgress for wm, q in queries.items()}
        for q in queries.values():
            q.stop()
        drop_topic(topic)
        for ck in ckpts:
            shutil.rmtree(ck, ignore_errors=True)

    # ---------------- compare with the truth ----------------
    # judge only windows that start after the run began: earlier ones hold nothing but late events
    # sent in the first seconds, when the queries had not set a watermark yet (first batch = no limit)
    first_win = (int(t0) // WINDOW + 1) * WINDOW
    windows = sorted({w for w, _ in truth if w >= first_win})
    winset = set(windows)
    sent_all = sum(truth.values())
    sent = sum(n for (w, _), n in truth.items() if w in winset)
    by_bucket = defaultdict(int)
    for (w, b), n in truth.items():
        if w in winset:
            by_bucket[b] += n

    summary, cols = [], {}
    for wm in wms:
        got = sum(n for (w, _), n in out[wm].items() if w in winset)
        dropped = sum((p.get("stateOperators") or [{}])[0].get("numRowsDroppedByWatermark", 0)
                      for p in progress[wm])
        state_rows = [(p.get("stateOperators") or [{}])[0].get("numRowsTotal", 0) for p in progress[wm]]
        state_mem = [(p.get("stateOperators") or [{}])[0].get("memoryUsedBytes", 0) for p in progress[wm]]
        lat = [emitted[wm][w] - (w + WINDOW) for w in windows if w in emitted[wm]]
        cols[wm] = dict(state_rows=state_rows)
        summary.append([f"{wm} s", got, sent - got, dropped, round(100 * got / max(sent, 1), 1),
                        round(sum(lat) / len(lat), 1) if lat else None, round(max(lat), 1) if lat else None,
                        max(state_rows or [0]), round(max(state_mem or [0]) / 1024, 1)])
        rep.metric(f"完整度（watermark {wm} s）", round(100 * got / max(sent, 1), 1), " %")
    rep.metric("送出事件", sent_all)
    rep.metric("遲到事件", sum(n for b, n in by_bucket.items() if b > 0))
    rep.metric("事件時間視窗", f"{WINDOW} 秒")

    rep.note("watermark = 目前看過的最大事件時間 − 容忍時間。視窗的結束時間一旦早於 watermark，Spark 就認定它不會再有資料："
             "append 模式把結果寫出一次並從 state 刪掉，之後才到的事件會被丟棄（numRowsDroppedByWatermark）。")
    rep.note("取捨：watermark 越長，收得越完整，但每個視窗要多等這麼久才有結果，而且 state 裡同時保留更多視窗。"
             "0 秒幾乎不等人，遲到的事件大多被丟掉；長 watermark 收得齊，但結果晚出來。")
    rep.note(f"每個查詢讀的是同一個 Kafka topic（3 個 partition），從 earliest 開始讀，各自有 checkpoint；"
             f"實驗結束後刪掉 topic 和 checkpoint。最後送出一筆未來 1 小時的事件，把 watermark 推過所有真實視窗，"
             f"讓 append 模式把最後幾個視窗也寫出來。")
    rep.note(f"比較範圍：實驗開始後的 {len(windows)} 個完整視窗（{sent:,} 筆事件）。開始前的視窗不算，"
             "因為第一個 micro-batch 還沒有 watermark，那幾秒送出的遲到事件全都會被收下，會讓結果失真。")
    rep.note("* Spark 丟棄列數取自 StreamingQueryProgress 的 numRowsDroppedByWatermark。它算的是進入 state 運算子前、"
             "已經在各 partition 先部分聚合過的列，不是原始事件數，所以會遠小於「少算的事件」。")

    rep.table("各 watermark 的比較", summary,
              columns=["watermark", "計入事件", "少算的事件", "Spark 丟棄列數*", "完整度 %", "平均結果延遲 s",
                       "最大結果延遲 s", "state 最多列數", "state 最高記憶體 KB"])

    rows = []
    for b, (name, lo, hi) in enumerate(BUCKETS):
        n = by_bucket.get(b, 0)
        row = [name, n]
        for wm in wms:
            got = sum(c for (w, bb), c in out[wm].items() if bb == b and w in winset)
            row.append(f"{100 * got / n:.0f} %" if n else "—")
        rows.append(row)
    rep.table("各遲到程度被計入的比例", rows, columns=["事件", "送出"] + [f"wm {wm} s" for wm in wms])

    lbl = [time.strftime("%H:%M:%S", time.localtime(w)) for w in windows]
    series = {}                                      # same colour per watermark as the state chart below
    for wm in wms:
        series[f"wm {wm} s"] = [sum(out[wm].get((w, b), 0) for b in range(4)) if w in emitted[wm] else None
                                for w in windows]
    series["真實筆數"] = [sum(truth.get((w, b), 0) for b in range(4)) for w in windows]
    rep.chart("每個 10 秒視窗的事件數：真實 vs 各 watermark 的輸出（收齊的線會和真實筆數重疊）", "line", lbl, series)
    rep.chart("完整度（%）", "bar", [f"{wm} s" for wm in wms], {"完整度 %": [s[4] for s in summary]})
    rep.chart("視窗結束後多久才有結果（平均秒數）", "bar", [f"{wm} s" for wm in wms],
              {"平均延遲 s": [s[5] for s in summary]})
    n = max(len(c["state_rows"]) for c in cols.values())
    rep.chart("state 裡保留的列數（每個 micro-batch）", "line", list(range(1, n + 1)),
              {f"wm {wm} s": c["state_rows"] + [None] * (n - len(c["state_rows"])) for wm, c in cols.items()})
    rep.save()
    spark.stop()
