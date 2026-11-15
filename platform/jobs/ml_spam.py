"""spark_mllib_101 / spam_detection  (lesson 0816 "TF-IDF")

spam_classifier.py from class: message length + RegexTokenizer ->
StopWordsRemover -> HashingTF(4000) -> IDF -> StringIndexer(label) ->
VectorAssembler -> LogisticRegression. Also scores your own sentences.
"""
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (HashingTF, IDF, RegexTokenizer, StopWordsRemover,
                                StringIndexer, VectorAssembler)
from pyspark.sql.functions import col, length, lower

from lab_common import COURSE, Report, get_spark, arg

if __name__ == "__main__":
    spark = get_spark("mllib spam_detection")
    rep = Report("垃圾簡訊辨識（TF-IDF + Logistic Regression）")
    custom = arg("text", "")

    with rep.step("load + preprocess"):
        data = spark.read.csv(f"{COURSE}/spark_mllib_101/spam_detection/data/sms_messages_with_labels.csv",
                              inferSchema=True, header=True)
        data = data.select(lower(col("message")).alias("message"),
                           length(col("message")).alias("length"), "label").dropna()

    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="message", outputCol="words", pattern="\\W+"),
        StopWordsRemover(inputCol="words", outputCol="stop_words_removed"),
        HashingTF(inputCol="stop_words_removed", outputCol="hashing_tf", numFeatures=4000),
        IDF(inputCol="hashing_tf", outputCol="tf_idf"),
        StringIndexer(inputCol="label", outputCol="label_indexed"),
        VectorAssembler(inputCols=["tf_idf", "length"], outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label_indexed"),
    ])

    with rep.step("train pipeline"):
        train_data, test_data = data.randomSplit([0.7, 0.3], seed=7)
        model = pipeline.fit(train_data)
        pred = model.transform(test_data).cache()

    ev = lambda m: MulticlassClassificationEvaluator(labelCol="label_indexed", predictionCol="prediction",
                                                     metricName=m).evaluate(pred)
    labels = model.stages[4].labels
    rep.metric("簡訊數", data.count())
    rep.metric("Accuracy", f"{ev('accuracy'):.1%}")
    rep.metric("F1", round(ev("f1"), 4))
    counts = data.groupBy("label").count().collect()
    rep.chart("ham / spam 比例", "pie", [r.label for r in counts], {"count": [r["count"] for r in counts]})
    cm = pred.groupBy("label", "prediction").count().orderBy("label", "prediction").collect()
    rep.table("混淆矩陣（label × prediction）",
              [(r.label, labels[int(r.prediction)], r["count"]) for r in cm], ["實際", "預測", "筆數"])

    samples = [custom] if custom else []
    samples += ["URGENT! You have won a 1 week FREE membership. Call 09061701461 now to claim",
                "Hey, are we still meeting for lunch tomorrow?",
                "Congratulations! Claim your free prize, text WIN to 80086",
                "I'll be home late tonight, don't wait for me"]
    s = spark.createDataFrame([(t.lower(), len(t), "ham") for t in samples], ["message", "length", "label"])
    out = model.transform(s).select("message", "prediction", "probability").collect()
    rep.table("試試看：自訂句子的判斷結果",
              [(r.message, labels[int(r.prediction)], f"{max(r.probability.toArray()):.1%}") for r in out],
              ["訊息", "判斷", "信心"])
    rep.save()
    spark.stop()
