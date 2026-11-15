"""spark_sql_101 / messages  (lesson 0729)

User message sentiment with the AFINN word list as a Python UDF
("Assume afinn module has been installed on all nodes").
"""
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from lab_common import COURSE, Report, get_spark


def afinn_score(text):
    from afinn import Afinn                       # loaded on each executor
    global _AF
    try:
        _AF
    except NameError:
        _AF = Afinn()
    return float(_AF.score(text or ""))


if __name__ == "__main__":
    spark = get_spark("spark_sql_101 messages_sentiment")
    rep = Report("推文情緒分析（AFINN UDF）")

    with rep.step("read csv"):
        msgs = spark.read.csv(f"{COURSE}/spark_sql_101/messages/data/user_messages.csv",
                              header=True, multiLine=True, escape='"')

    with rep.step("score with UDF"):
        score = F.udf(afinn_score, FloatType())
        scored = msgs.withColumn("score", score("message")).cache()
        n = scored.count()

    with rep.step("aggregate per user"):
        per_user = scored.groupBy("user").agg(F.count("*").alias("messages"),
                                              F.round(F.avg("score"), 2).alias("avg_score"))
        happiest = per_user.filter("messages >= 3").orderBy(F.desc("avg_score")).limit(10)
        saddest = per_user.filter("messages >= 3").orderBy("avg_score").limit(10)
        dist = scored.withColumn("bucket", F.when(F.col("score") > 0, "正面")
                                 .when(F.col("score") < 0, "負面").otherwise("中性")) \
                     .groupBy("bucket").count().collect()

    rep.metric("訊息數", n)
    rep.metric("使用者數", per_user.count())
    rep.metric("平均情緒分數", round(scored.agg(F.avg("score")).first()[0], 3))
    rep.chart("情緒分布", "pie", [r.bucket for r in dist], {"count": [r["count"] for r in dist]})
    rep.table("最正面的使用者（≥3 則）", happiest)
    rep.table("最負面的使用者（≥3 則）", saddest)
    rep.table("最正面的訊息", scored.orderBy(F.desc("score")).select("user", "score", "message"), limit=8)
    rep.table("最負面的訊息", scored.orderBy("score").select("user", "score", "message"), limit=8)
    rep.save()
    spark.stop()
