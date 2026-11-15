"""spark_streaming_101 / ec_logs  (lesson 0804-0805: ec_logs_realtime_states.py)

Real-time per-visitor stats from the logs_stream topic, over a sliding window of
60 s that refreshes every 5 s — the same window(60, 5) as the DStream version —
written back to Kafka topic user_activity_stream as "visitor,sales,add_to_carts,page_views".

Rewritten with Structured Streaming (DStream + spark-streaming-kafka-0-8 were removed
in Spark 3). foreachBatch also saves a snapshot for the portal's live view.
"""
import datetime
import json
import os
import time

from pyspark.sql import functions as F

from lab_common import WORK, get_spark

SNAP = f"{WORK}/stream/ec_logs_latest.json"


STATE = {}            # (window_end, visitor) -> latest counts; update mode only sends changed rows


def current_window(ends):
    """The sliding window that covers the last 60 s: the earliest one that has not ended yet."""
    now = datetime.datetime.now()
    future = [e for e in ends if e > now]
    return min(future) if future else max(ends)


def publish(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    for r in rows:
        STATE[(r.window.end, r.visitor)] = r
    oldest = datetime.datetime.now() - datetime.timedelta(seconds=120)
    for k in [k for k in STATE if k[0] < oldest]:
        del STATE[k]
    latest_end = current_window({k[0] for k in STATE})
    cur = [r for (end, _), r in STATE.items() if end == latest_end]
    cur.sort(key=lambda r: -r.number_of_events)
    totals = {k: sum(getattr(r, k) for r in cur) for k in
              ("number_of_sales", "number_of_add_to_carts", "number_of_page_views", "number_of_events")}
    snap = {"batch": batch_id, "window_end": str(latest_end), "updated": time.time(), "totals": totals,
            "visitors": len(cur),
            "top": [[r.visitor, r.number_of_sales, r.number_of_add_to_carts, r.number_of_page_views,
                     r.number_of_events] for r in cur[:15]]}
    with open(SNAP + ".tmp", "w") as f:
        json.dump(snap, f)
    os.replace(SNAP + ".tmp", SNAP)
    # send "visitor,sales,carts,views" lines to Kafka, like the course's output topic
    batch_df.where(F.col("window.end") == latest_end) \
        .select(F.col("visitor").alias("key"),
                F.concat_ws(",", "visitor", "number_of_sales", "number_of_add_to_carts",
                            "number_of_page_views").alias("value")) \
        .write.format("kafka").option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "user_activity_stream").save()


if __name__ == "__main__":
    spark = get_spark("streaming ec_logs_realtime_states")
    raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092") \
               .option("subscribe", "logs_stream").option("startingOffsets", "latest").load()
    arr = F.split(F.col("value").cast("string"), ",")
    events = raw.select(F.to_timestamp(arr[0]).alias("timestamp"), arr[1].alias("referer"),
                        arr[2].alias("action"), arr[3].alias("visitor"), arr[4].alias("page"),
                        arr[5].alias("product"))
    stats = events.withWatermark("timestamp", "10 seconds") \
        .groupBy(F.window("timestamp", "60 seconds", "5 seconds"), "visitor") \
        .agg(F.sum((F.col("action") == "sale").cast("int")).alias("number_of_sales"),
             F.sum((F.col("action") == "add_to_cart").cast("int")).alias("number_of_add_to_carts"),
             F.sum((F.col("action") == "page_view").cast("int")).alias("number_of_page_views"),
             F.count("*").alias("number_of_events"))
    q = stats.writeStream.outputMode("update").trigger(processingTime="5 seconds") \
             .option("checkpointLocation", f"{WORK}/checkpoints/ec_logs_{int(time.time())}") \
             .foreachBatch(publish).start()
    q.awaitTermination()
