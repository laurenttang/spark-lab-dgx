"""spark101 / wordcount  (lesson 0715-0716)

The very first Spark program of the course: count words in War and Peace
with the RDD API (flatMap -> map -> reduceByKey), using a regex to split
words cleanly ("使用 regex 使斷詞更乾淨").
"""
import re

from lab_common import COURSE, Report, get_spark, arg

if __name__ == "__main__":
    spark = get_spark("spark101 wordcount")
    sc = spark.sparkContext
    rep = Report("WordCount：《戰爭與和平》")
    top_n = arg("top", 20, int)

    with rep.step("read text"):
        lines = sc.textFile(f"{COURSE}/spark101/wordcount/data/war_and_peace.txt",
                            minPartitions=16)

    with rep.step("flatMap / map / reduceByKey"):
        words = lines.flatMap(lambda line: re.compile(r"\W+", re.UNICODE).split(line.lower())) \
                     .filter(lambda w: w != "")
        pairs = words.map(lambda w: (w, 1))
        word_counts = pairs.reduceByKey(lambda x, y: x + y)
        word_counts.cache()
        total_words = words.count()
        distinct_words = word_counts.count()

    with rep.step("sort and take top words"):
        top = word_counts.sortBy(lambda p: p[1], ascending=False).take(top_n)
        # ignore very common English stop words for a more interesting chart
        stop = {"the", "and", "to", "of", "a", "he", "in", "his", "that", "was", "with",
                "had", "it", "her", "not", "at", "him", "as", "s", "on", "but", "for",
                "she", "i", "you", "is", "they", "all", "from", "by", "said", "were",
                "be", "what", "who", "this", "have", "which", "one", "there", "so"}
        top_content = word_counts.filter(lambda p: p[0] not in stop and len(p[0]) > 2) \
                                 .sortBy(lambda p: p[1], ascending=False).take(top_n)

    rep.metric("文字行數", lines.count())
    rep.metric("總字數", total_words)
    rep.metric("不重複字數", distinct_words)
    rep.metric("RDD partitions", lines.getNumPartitions())
    rep.chart(f"出現最多的 {top_n} 個字", "hbar", [w for w, _ in top], {"次數": [c for _, c in top]})
    rep.chart("排除常見虛詞後（人名、主題字浮現）", "hbar",
              [w for w, _ in top_content], {"次數": [c for _, c in top_content]})
    rep.table("Top words", top, ["word", "count"])
    rep.save()
    spark.stop()
