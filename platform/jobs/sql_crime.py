"""spark_sql_101 / crime  (lessons 0725-0729)

London crime by LSOA (Kaggle, 1.35M rows): DataFrame API + Spark SQL,
groupBy/agg, column expressions, temp views.
"""
from pyspark.sql import functions as F

from lab_common import COURSE, Report, get_spark

if __name__ == "__main__":
    spark = get_spark("spark_sql_101 london_crime")
    rep = Report("倫敦犯罪資料分析（134 萬筆，Spark SQL）")

    with rep.step("read csv (inferSchema)"):
        crime = spark.read.csv(f"{COURSE}/spark_sql_101/crime/data/london_crime_by_lsoa.csv",
                               header=True, inferSchema=True).cache()
        crime.createOrReplaceTempView("crime")
        n = crime.count()

    with rep.step("by year (SQL)"):
        by_year = spark.sql("""
            SELECT year, SUM(value) AS total FROM crime
            GROUP BY year ORDER BY year""").collect()

    with rep.step("by borough (DataFrame API)"):
        by_borough = crime.groupBy("borough").agg(F.sum("value").alias("total")) \
                          .orderBy(F.desc("total")).limit(15).collect()

    with rep.step("by category per year (pivot)"):
        cats = crime.groupBy("major_category").agg(F.sum("value").alias("t")) \
                    .orderBy(F.desc("t")).limit(6).collect()
        top_cats = [c.major_category for c in cats]
        pivot = crime.filter(F.col("major_category").isin(top_cats)) \
                     .groupBy("year").pivot("major_category", top_cats).sum("value") \
                     .orderBy("year").collect()

    with rep.step("year-over-year change by borough"):
        yoy = spark.sql("""
            WITH t AS (SELECT borough, year, SUM(value) v FROM crime GROUP BY borough, year)
            SELECT a.borough, a.v AS y2015, b.v AS y2016,
                   ROUND((b.v - a.v) * 100.0 / a.v, 1) AS change_pct
            FROM t a JOIN t b ON a.borough = b.borough AND a.year = 2015 AND b.year = 2016
            ORDER BY change_pct DESC""")

    rep.metric("資料筆數", n)
    rep.metric("行政區", crime.select("borough").distinct().count())
    rep.metric("犯罪件數總和", int(sum(r.total for r in by_year)))
    rep.chart("每年犯罪件數", "line", [r.year for r in by_year], {"件數": [r.total for r in by_year]})
    rep.chart("犯罪件數最多的 15 個行政區", "hbar", [r.borough for r in by_borough],
              {"件數": [r.total for r in by_borough]})
    rep.chart("主要犯罪類別逐年趨勢", "line", [r.year for r in pivot],
              {c: [r[c] for r in pivot] for c in top_cats})
    rep.table("2015 → 2016 各區變化", yoy, limit=33)
    rep.save()
    spark.stop()
