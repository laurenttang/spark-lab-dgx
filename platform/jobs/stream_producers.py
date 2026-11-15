"""Kafka producers for the streaming demos (lesson 0804-0805).

ec_logs      : like ec_logs_producer.py — random e-commerce events
               "timestamp,referer-N,action,visitor-N,page-N,product-N" -> topic logs_stream
tweets       : like user_tweets_producer.py — replay Sentiment140 tweets
               key=user, value=message -> topic tweets_stream

Usage: python stream_producers.py ec_logs|tweets --rate 20
"""
import csv
import datetime as dt
import json
import random
import sys
import time

from kafka import KafkaProducer

from lab_common import COURSE, WORK, arg

BOOTSTRAP = "kafka:9092"
STATS = f"{WORK}/stream"
VISITORS = list(range(1, 201))
VISITOR_W = [1 / (i ** 0.7) for i in VISITORS]          # a few regulars, a long tail of visitors


def ec_events():
    actions = ["page_view"] * 14 + ["add_to_cart"] * 4 + ["sale"] * 2
    while True:
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield None, (f"{now},referer-{random.randint(1, 15)},{random.choice(actions)},"
                     f"visitor-{random.choices(VISITORS, VISITOR_W)[0]},page-{random.randint(1, 200)},"
                     f"product-{random.randint(1, 100)}")


def tweets():
    path = f"{COURSE}/spark_streaming_101/simple_sentiment_analysis_demo/data/messages.csv"
    while True:
        with open(path, newline="", encoding="latin-1") as csvfile:
            rows = csv.reader(csvfile)
            users = [f"user_{i:02d}" for i in range(1, 31)]   # 30 regular users -> visible per-user scores
            for row in rows:
                if len(row) < 6:
                    continue
                yield random.choice(users), row[5]


if __name__ == "__main__":
    kind = sys.argv[1]
    rate = arg("rate", 20, int)
    topic = {"ec_logs": "logs_stream", "tweets": "tweets_stream"}[kind]
    gen = ec_events() if kind == "ec_logs" else tweets()
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    sent, t0 = 0, time.time()
    while True:
        batch_start = time.time()
        for _ in range(rate):
            key, value = next(gen)
            producer.send(topic, key=key.encode() if key else None, value=value.encode("utf-8"))
            sent += 1
        producer.flush()
        with open(f"{STATS}/{kind}_producer.json", "w") as f:
            json.dump({"sent": sent, "rate": rate, "uptime": round(time.time() - t0), "last": value[:160]}, f)
        if sent // rate % 60 == 1:                       # once a minute: the host never reboots, keep logs small
            print("Sending data...", sent, flush=True)
        time.sleep(max(0.0, 1 - (time.time() - batch_start)))
