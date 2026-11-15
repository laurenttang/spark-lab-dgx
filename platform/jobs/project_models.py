"""期末專題 / ec_web_logs_analysis_1  steps 4-5  (lesson 0819)

model_gender_prediction.py    : RandomForest(gender) on product_category_id, device_type, connect_type
model_age_group_prediction.py : StringIndexer(age_group) + RandomForest
Both models are saved (model.save) for the analysis_2 jobs to load.
"""
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from lab_common import WORK, Report, get_spark

BASE = f"{WORK}/ec_web_logs_analysis"
FEATURES = ["product_category_id", "device_type", "connect_type"]

if __name__ == "__main__":
    spark = get_spark("ec_web_logs_analysis_1 models")
    rep = Report("期末專題 ②：訓練性別 / 年齡層預測模型")
    logs = spark.read.parquet(f"{BASE}/data")
    vec = VectorAssembler(inputCols=FEATURES, outputCol="features")
    scores = {}

    with rep.step("gender model"):
        feature_prep = logs.select(*FEATURES, "gender").where(F.col("gender").isNotNull())
        train_data, test_data = vec.transform(feature_prep).randomSplit([0.7, 0.3], seed=11)
        model = RandomForestClassifier(featuresCol="features", labelCol="gender", numTrees=10, maxDepth=10).fit(train_data)
        pred = model.transform(test_data)
        scores["gender"] = [MulticlassClassificationEvaluator(labelCol="gender", metricName=m).evaluate(pred)
                            for m in ("f1", "accuracy")]
        model.write().overwrite().save(f"{BASE}/models/model_gender_prediction")

    with rep.step("age_group model"):
        feature_prep = logs.select(*FEATURES, "age_group").where(F.col("age_group").isNotNull())
        indexer = StringIndexer(inputCol="age_group", outputCol="age_group_indexed").fit(feature_prep)
        indexer.write().overwrite().save(f"{BASE}/models/age_group_indexer")
        train_data, test_data = vec.transform(indexer.transform(feature_prep)).randomSplit([0.7, 0.3], seed=11)
        model = RandomForestClassifier(featuresCol="features", labelCol="age_group_indexed",
                                       numTrees=10, maxDepth=10).fit(train_data)
        pred = model.transform(test_data)
        scores["age_group"] = [MulticlassClassificationEvaluator(labelCol="age_group_indexed", metricName=m)
                               .evaluate(pred) for m in ("f1", "accuracy")]
        model.write().overwrite().save(f"{BASE}/models/model_age_group_prediction")

    known = logs.agg(F.count("gender").alias("g"), F.count("age_group").alias("a"), F.count("*").alias("n")).first()
    rep.metric("性別 已知 / 全部", f"{known.g:,} / {known.n:,}")
    rep.metric("年齡層 已知 / 全部", f"{known.a:,} / {known.n:,}")
    rep.metric("性別模型 Accuracy", f"{scores['gender'][1]:.1%}")
    rep.metric("年齡層模型 Accuracy", f"{scores['age_group'][1]:.1%}")
    rep.chart("模型評估", "bar", ["gender", "age_group"],
              {"F1": [scores["gender"][0], scores["age_group"][0]],
               "Accuracy": [scores["gender"][1], scores["age_group"][1]]})
    rep.table("age_group 標籤對照（StringIndexer）", list(enumerate(indexer.labels)), ["index", "age_group"])
    rep.note("模型已存到 /work/ec_web_logs_analysis/models/，下一步「③ 使用者輪廓」會載入使用。")
    rep.save()
    spark.stop()
