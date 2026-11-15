"""spark_mllib_101 / titanic  (lesson 0816 "Supervised learning")

ML prediction pipeline from class: select + dropna -> StringIndexer ->
OneHotEncoder -> VectorAssembler -> LogisticRegression / RandomForest,
evaluated with F1 + accuracy.
"""
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

from lab_common import COURSE, Report, get_spark

if __name__ == "__main__":
    spark = get_spark("mllib titanic")
    rep = Report("鐵達尼號生還預測（Logistic Regression vs Random Forest）")

    with rep.step("prepare data"):
        raw = spark.read.csv(f"{COURSE}/spark_mllib_101/titanic/titanic.csv", inferSchema=True, header=True)
        data = raw.select("Survived", "Pclass", "Sex", "Age", "SibSp", "Parch", "Fare", "Embarked").dropna()

    with rep.step("feature engineering"):
        data = StringIndexer(inputCol="Sex", outputCol="SexIndex").fit(data).transform(data)
        data = OneHotEncoder(inputCol="SexIndex", outputCol="SexVec").fit(data).transform(data)
        data = StringIndexer(inputCol="Embarked", outputCol="EmbarkIndex").fit(data).transform(data)
        data = OneHotEncoder(inputCol="EmbarkIndex", outputCol="EmbarkVec").fit(data).transform(data)
        final_data = VectorAssembler(inputCols=["Pclass", "SexVec", "Age", "SibSp", "Parch", "Fare", "EmbarkVec"],
                                     outputCol="features").transform(data)
        train_data, test_data = final_data.randomSplit([0.7, 0.3], seed=42)

    f1 = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="f1")
    acc = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")
    results = {}
    for name, clf in [("Logistic Regression", LogisticRegression(featuresCol="features", labelCol="Survived")),
                      ("Random Forest", RandomForestClassifier(featuresCol="features", labelCol="Survived",
                                                               numTrees=10, maxDepth=10, seed=42))]:
        with rep.step(f"train {name}"):
            model = clf.fit(train_data)
            pred = model.transform(test_data)
            results[name] = (f1.evaluate(pred), acc.evaluate(pred), model, pred)

    rf_model = results["Random Forest"][2]
    imp = list(zip(["Pclass", "Sex(male)", "Age", "SibSp", "Parch", "Fare", "Emb:S", "Emb:C"],
                   rf_model.featureImportances.toArray()))
    rep.metric("訓練筆數", train_data.count())
    rep.metric("測試筆數", test_data.count())
    for name, (f, a, _, _) in results.items():
        rep.metric(f"{name} 準確率", f"{a:.1%}")
    rep.chart("模型比較", "bar", list(results), {"F1": [r[0] for r in results.values()],
                                               "Accuracy": [r[1] for r in results.values()]})
    rep.chart("Random Forest 特徵重要度", "hbar", [i[0] for i in imp], {"importance": [float(i[1]) for i in imp]})
    surv = data.groupBy("Sex", "Pclass").avg("Survived").orderBy("Sex", "Pclass").collect()
    rep.chart("各艙等 × 性別 生還率", "bar", [f"{r.Sex} / {r.Pclass}等艙" for r in surv],
              {"生還率": [r[2] for r in surv]})
    rep.table("預測結果（Random Forest，測試集）",
              results["Random Forest"][3].select("Survived", "prediction", "Pclass", "Sex", "Age", "Fare"), limit=20)
    rep.save()
    spark.stop()
