"""spark_mllib_101 / movies  (lesson 0816 "Predict 2_ALS")

movie_recommandation_cf_als.py from class: ALS collaborative filtering on
MovieLens ratings, RMSE evaluation, recommendForAllUsers / Items.
"""
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from lab_common import COURSE, Report, get_spark, arg

if __name__ == "__main__":
    spark = get_spark("mllib movie_recommendation_als")
    rep = Report("電影推薦系統（ALS 協同過濾）")
    user = arg("user", 1, int)

    with rep.step("load"):
        final_data = spark.read.csv(f"{COURSE}/spark_mllib_101/movies/ratings.csv", inferSchema=True, header=True)
        movies = spark.read.csv(f"{COURSE}/spark_mllib_101/movies/movies.csv", inferSchema=True, header=True)
        train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=1)

    with rep.step("train ALS"):
        als = ALS(maxIter=5, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop", seed=1)
        model = als.fit(train_data)
        predicted_test_data = model.transform(test_data)
        rmse = RegressionEvaluator(predictionCol="prediction", labelCol="rating",
                                   metricName="rmse").evaluate(predicted_test_data)

    with rep.step("recommend"):
        recs = model.recommendForUserSubset(spark.createDataFrame([(user,)], ["userId"]), 10) \
                    .select(F.explode("recommendations").alias("r")) \
                    .select(F.col("r.movieId").alias("movieId"), F.round("r.rating", 2).alias("score")) \
                    .join(movies, "movieId").orderBy(F.desc("score"))
        liked = final_data.filter(F.col("userId") == user).join(movies, "movieId") \
                          .orderBy(F.desc("rating"), "title").select("title", "rating")

    rep.metric("評分筆數", final_data.count())
    rep.metric("使用者 / 電影", f"{final_data.select('userId').distinct().count()} / "
                               f"{final_data.select('movieId').distinct().count()}")
    rep.metric("測試集 RMSE", round(rmse, 4))
    rep.metric("推薦對象 userId", user)
    rec_rows = recs.collect()
    rep.chart(f"推薦給 user {user} 的電影", "hbar", [r.title for r in rec_rows], {"預測評分": [r.score for r in rec_rows]})
    rep.table(f"user {user} 自己評過高分的電影", liked, limit=10)
    rep.table("預測 vs 實際（測試集）", predicted_test_data.join(movies, "movieId")
              .select("userId", "title", "rating", F.round("prediction", 2).alias("prediction")), limit=15)
    rep.save()
    spark.stop()
