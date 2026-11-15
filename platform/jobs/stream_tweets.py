"""spark_streaming_101 / simple_sentiment_analysis_demo  (lesson 0804: user_tweets_realtime_scores.py)

Score every tweet from tweets_stream with AFINN and rank users by their total
sentiment over a 5-minute window sliding every 20 s (the course's window(300, 20)).
Structured Streaming version; the portal shows the live ranking.
"""
import datetime
import json
import os
import time

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from lab_common import WORK, get_spark

SNAP = f"{WORK}/stream/tweets_latest.json"


def afinn_score(text):
    global _AF
    try:
        _AF
    except NameError:
        from afinn import Afinn
        _AF = Afinn()
    return float(_AF.score(text or ""))


STATE = {}            # (window_end, user) -> latest row; update mode only sends changed rows


def publish(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    for r in rows:
        STATE[(r.window.end, r.user)] = r
    now = datetime.datetime.now()
    for k in [k for k in STATE if k[0] < now - datetime.timedelta(seconds=60)]:
        del STATE[k]
    ends = {k[0] for k in STATE}
    future = [e for e in ends if e > now]
    latest_end = min(future) if future else max(ends)     # window covering the last 5 minutes
    cur = sorted((r for (e, _), r in STATE.items() if e == latest_end), key=lambda r: -r.score)
    snap = {"batch": batch_id, "window_end": str(latest_end), "updated": time.time(),
            "users": [[r.user, round(r.score, 1), r.tweets, r.sample] for r in cur]}
    with open(SNAP + ".tmp", "w") as f:
        json.dump(snap, f, ensure_ascii=False)
    os.replace(SNAP + ".tmp", SNAP)


if __name__ == "__main__":
    spark = get_spark("streaming user_tweets_realtime_scores")
    score = F.udf(afinn_score, FloatType())
    raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092") \
               .option("subscribe", "tweets_stream").option("startingOffsets", "latest").load()
    tweets = raw.select(F.col("key").cast("string").alias("user"), F.col("value").cast("string").alias("message"),
                        F.col("timestamp"))
    ranked = tweets.withColumn("s", score("message")).withWatermark("timestamp", "20 seconds") \
        .groupBy(F.window("timestamp", "300 seconds", "20 seconds"), "user") \
        .agg(F.sum("s").alias("score"), F.count("*").alias("tweets"), F.last("message").alias("sample"))
    q = ranked.writeStream.outputMode("update").trigger(processingTime="5 seconds") \
              .option("checkpointLocation", f"{WORK}/checkpoints/tweets_{int(time.time())}") \
              .foreachBatch(publish).start()
    q.awaitTermination()
