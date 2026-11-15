"""spark101 / movies_similarity  (Spark Introduction deck: Similar_movies.py)

Item-based collaborative filtering on MovieLens 100k:
self-join every user's ratings to build movie pairs, then compute cosine
similarity between movies. Shows how a shuffle-heavy job spreads across
the cluster (watch the stages in the Spark UI).
"""
from pyspark.sql import functions as F

from lab_common import COURSE, Report, get_spark, arg

if __name__ == "__main__":
    spark = get_spark("spark101 similar_movies")
    rep = Report("相似電影推薦（餘弦相似度）")
    movie_id = arg("movie", 50, int)            # 50 = Star Wars (1977)
    min_pairs = arg("min_pairs", 50, int)

    with rep.step("load"):
        ratings = spark.read.csv(f"{COURSE}/spark101/movies_similarity/data/movie_ratings/movie_ratings",
                                 sep="\t", schema="user INT, movie INT, rating DOUBLE, ts LONG")
        names = spark.read.csv(f"{COURSE}/spark101/movies_similarity/data/movies/movies",
                               schema="movie INT, title STRING")

    with rep.step("self-join -> movie pairs"):
        r1, r2 = ratings.alias("a"), ratings.alias("b")
        pairs = r1.join(r2, (F.col("a.user") == F.col("b.user")) & (F.col("a.movie") < F.col("b.movie"))) \
                  .select(F.col("a.movie").alias("m1"), F.col("b.movie").alias("m2"),
                          F.col("a.rating").alias("x"), F.col("b.rating").alias("y"))

    with rep.step("cosine similarity"):
        sim = pairs.groupBy("m1", "m2").agg(
            F.count("*").alias("num_pairs"),
            (F.sum(F.col("x") * F.col("y")) /
             (F.sqrt(F.sum(F.col("x") ** 2)) * F.sqrt(F.sum(F.col("y") ** 2)))).alias("score"))
        target = sim.filter(((F.col("m1") == movie_id) | (F.col("m2") == movie_id)) &
                            (F.col("num_pairs") >= min_pairs)) \
                    .withColumn("other", F.when(F.col("m1") == movie_id, F.col("m2")).otherwise(F.col("m1"))) \
                    .join(names, F.col("other") == names.movie) \
                    .orderBy(F.desc("score")).limit(10) \
                    .select("title", F.round("score", 4).alias("score"), "num_pairs")
        top = target.collect()

    title = names.filter(F.col("movie") == movie_id).first()
    rep.metric("評分筆數", ratings.count())
    rep.metric("目標電影", title.title if title else movie_id)
    rep.metric("最少共同評分人數", min_pairs)
    rep.chart(f"與「{title.title if title else movie_id}」最相似的電影", "hbar",
              [r.title for r in top], {"cosine score": [r.score for r in top]})
    rep.table("Similar movies", [tuple(r) for r in top], ["title", "score", "num_pairs"])
    rep.save()
    spark.stop()
