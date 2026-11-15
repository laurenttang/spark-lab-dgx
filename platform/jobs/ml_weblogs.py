"""spark_mllib_101 / web_logs_analysis  (lesson 0816 "Predict 1" + "Unsupervised learning")

Two models on 2.1M e-commerce web-log rows (May 2019, Taiwan):
  1. web_log_gender_model.py: RandomForest predicts gender from
     product_category_id / device_type / connection_type
  2. web_log_user_location_clustering.py: KMeans(k=5) on each device's
     lat/lon to find the places a user goes frequently
"""
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from lab_common import COURSE, Report, get_spark, arg

if __name__ == "__main__":
    spark = get_spark("mllib web_logs")
    rep = Report("電商 Web Logs：性別預測 + 使用者常去地點分群")
    num_users = arg("users", 5, int)

    with rep.step("read 8 CSV files"):
        logs = spark.read.csv(f"{COURSE}/spark_mllib_101/web_logs_analysis/data/", header=True, inferSchema=True)
        logs = logs.withColumnRenamed("connection_type", "connect_type").cache()
        n = logs.count()

    with rep.step("RandomForest gender model"):
        feature_prep = logs.select("product_category_id", "device_type", "connect_type", "gender") \
                           .where(F.col("gender").isNotNull())
        final_data = VectorAssembler(inputCols=["product_category_id", "device_type", "connect_type"],
                                     outputCol="features").transform(feature_prep)
        train_data, test_data = final_data.randomSplit([0.7, 0.3], seed=3)
        model = RandomForestClassifier(featuresCol="features", labelCol="gender",
                                       numTrees=10, maxDepth=10).fit(train_data)
        pred = model.transform(test_data)
        ev = lambda m: MulticlassClassificationEvaluator(labelCol="gender", predictionCol="prediction",
                                                         metricName=m).evaluate(pred)
        f1, acc = ev("f1"), ev("accuracy")

    with rep.step(f"KMeans locations for {num_users} heaviest users"):
        top_users = [r.device_id for r in logs.groupBy("device_id").count()
                     .orderBy(F.desc("count")).limit(num_users).collect()]
        clusters = []
        for u in top_users:
            user_locations = logs.select("device_id", "lat", "lon").filter(F.col("device_id") == u).dropna()
            user_data = VectorAssembler(inputCols=["lat", "lon"], outputCol="features").transform(user_locations)
            km = KMeans(k=5, seed=1).fit(user_data)
            c = km.transform(user_data).groupBy("prediction") \
                  .agg(F.avg("lat").alias("lat"), F.avg("lon").alias("lon"), F.count("*").alias("location_count")) \
                  .orderBy(F.desc("location_count")).collect()
            clusters += [(u[:8], r.prediction, round(r.lat, 5), round(r.lon, 5), r.location_count) for r in c]

    rep.metric("Log 筆數", n)
    rep.metric("裝置數", logs.select("device_id").distinct().count())
    rep.metric("性別模型 Accuracy", f"{acc:.1%}")
    rep.metric("性別模型 F1", round(f1, 4))
    hourly = logs.groupBy(F.hour("timestamp").alias("h")).count().orderBy("h").collect()
    rep.chart("每小時活動量", "bar", [f"{r.h:02d}" for r in hourly], {"events": [r["count"] for r in hourly]})
    rep.raw("map_points", [{"lat": c[2], "lon": c[3], "label": f"{c[0]} #{c[1]}", "size": c[4], "group": c[0]}
                           for c in clusters])
    rep.table("KMeans 常去地點（每位使用者 5 群）", clusters, ["device", "cluster", "lat", "lon", "count"])
    rep.note("資料是模擬的電商 log（依原課程資料的統計分布產生）；原課程用真實課程資料時，同一模型 F1 ≈ 0.925。")
    rep.save()
    spark.stop()
