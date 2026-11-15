"""hive101  (lesson 0730: Hive + Spark SQL integration)

The original lesson ran these HiveQL files against a Hive Metastore on
MySQL + HDFS. Spark SQL runs the same SQL directly; tables live in the
Spark catalog (warehouse under /work/warehouse).

  stocks/create_table.sql + count.sql       -> external table + GROUP BY/HAVING
  join/joins.sql                            -> inner / outer / map(broadcast) joins
  page_views/timeonsite.sql (+ .py)         -> Hive TRANSFORM script, rewritten
                                               as a window function
  save_result_to_table/*.sql                -> CREATE TABLE AS SELECT
"""
from lab_common import COURSE, Report, get_spark

H = f"{COURSE}/hive101"

if __name__ == "__main__":
    spark = get_spark("hive101 sql")
    rep = Report("Hive 範例（Spark SQL 執行原 HiveQL）")

    with rep.step("create tables"):
        spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW stocks
            (symbol STRING, day STRING, open FLOAT, high FLOAT, low FLOAT,
             close FLOAT, volume INT, adj_close FLOAT)
            USING csv OPTIONS (path '{H}/stocks/data/stocks.txt')""")
        spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW sales (name STRING, id INT)
            USING csv OPTIONS (path '{H}/join/data/sales.txt', sep '\t')""")
        spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW things (id INT, name STRING)
            USING csv OPTIONS (path '{H}/join/data/things.txt', sep '\t')""")
        spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW page_views
            (logtime STRING, userid INT, ip STRING, page STRING, ref STRING,
             os STRING, os_ver STRING, agent STRING)
            USING csv OPTIONS (path '{H}/page_views/data', sep '\t')""")

    with rep.step("stocks/count.sql"):
        q = ("SELECT symbol, count(*) AS num_records, AVG(open) AS avg_open, MAX(open) AS max_open "
             "FROM stocks WHERE symbol != 'CSCO' GROUP BY symbol HAVING avg_open >= 100 ORDER BY symbol")
        rep.table("stocks/count.sql — " + " ".join(q.split())[:120], spark.sql(q))
        rep.table("symbol, count(*)", spark.sql("SELECT symbol, count(*) AS n FROM stocks GROUP BY symbol"))

    with rep.step("join/joins.sql"):
        for label, sql in [
            ("JOIN", "SELECT sales.*, things.* FROM sales JOIN things ON (sales.id = things.id)"),
            ("LEFT OUTER JOIN", "SELECT sales.*, things.* FROM sales LEFT OUTER JOIN things ON (sales.id = things.id)"),
            ("FULL OUTER JOIN", "SELECT sales.*, things.* FROM sales FULL OUTER JOIN things ON (sales.id = things.id)"),
            ("/*+ MAPJOIN(things) */  (= broadcast join)",
             "SELECT /*+ MAPJOIN(things) */ sales.*, things.* FROM sales JOIN things ON (sales.id = things.id)"),
        ]:
            rep.table(label, spark.sql(sql))

    with rep.step("page_views time on site (window function)"):
        tos = spark.sql("""
            WITH t AS (
              SELECT userid, to_timestamp(logtime, 'MM/dd/yyyy HH:mm:ss') AS ts FROM page_views),
            d AS (
              SELECT userid, ts,
                     unix_timestamp(ts) - unix_timestamp(LAG(ts) OVER (PARTITION BY userid ORDER BY ts)) AS gap
              FROM t)
            SELECT userid, COALESCE(SUM(gap), 0) AS timeonsite_sec, COUNT(*) AS views
            FROM d GROUP BY userid ORDER BY timeonsite_sec DESC""")
        rep.table("timeonsite（原 TRANSFORM USING './timeonsite.py'）", tos, limit=20)
        pages = spark.sql("SELECT page, count(*) AS views FROM page_views GROUP BY page ORDER BY views DESC").collect()
        rep.chart("各頁面瀏覽數", "bar", [r.page for r in pages], {"views": [r.views for r in pages]})

    with rep.step("save_result_to_table (CTAS)"):
        spark.sql("DROP TABLE IF EXISTS stocks_stats")
        spark.sql("""CREATE TABLE stocks_stats USING parquet AS
            SELECT symbol, count(*) AS record_count, AVG(open) AS avg_open, STDDEV(high) AS std_high
            FROM stocks GROUP BY symbol""")
        rep.table("stocks_stats（CREATE TABLE AS SELECT）", spark.table("stocks_stats"))

    rep.metric("page_views 筆數", spark.table("page_views").count())
    rep.metric("stocks 筆數", spark.table("stocks").count())
    rep.note("原課程：Hive 1.2.1 + MySQL Metastore + HDFS；這裡用 Spark 內建 catalog，SQL 語法不變。")
    rep.save()
    spark.stop()
