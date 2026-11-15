"""期末專題 / ec_web_logs_analysis_2 + dashboard  (lessons 0819-0820)

ec_web_logs_device_location_analysis.py   : KMeans on each device's lat/lon -> places it goes
ec_web_logs_device_gender_analysis.py     : load gender model, predict every event, majority vote per device
ec_web_logs_device_age_group_analysis.py  : same for age group
ec_web_logs_device_activities_analysis.py : hourly PV / UV
taipei_eastern_district_user_analysis.py  : who visits Taipei East District, with inferred profile

The course wrote these tables to MySQL and built the "EC User Locations" dashboard in
Tableau. Here they are saved as Parquet tables and drawn by the portal.

Modernised: the class looped `for device in all_device_ids: KMeans(...)` one device at a
time on the driver; applyInPandas runs one scikit-learn KMeans per device in parallel
across all executors.
"""
import pandas as pd
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql import Window
from pyspark.sql import functions as F

from lab_common import WORK, Report, get_spark
from project_etl import GEO

BASE = f"{WORK}/ec_web_logs_analysis"
FEATURES = ["product_category_id", "device_type", "connect_type"]


def kmeans_per_device(pdf: pd.DataFrame) -> pd.DataFrame:
    from sklearn.cluster import KMeans
    pts = pdf[["lat", "lon"]].dropna()
    k = min(5, len(pts.drop_duplicates()))
    if k == 0:
        return pd.DataFrame(columns=["device_id", "avg_lat", "avg_lon", "lat_lon_count"])
    labels = KMeans(n_clusters=k, n_init=3, random_state=0).fit_predict(pts)
    g = pts.assign(c=labels).groupby("c").agg(avg_lat=("lat", "mean"), avg_lon=("lon", "mean"),
                                               lat_lon_count=("lat", "size")).reset_index(drop=True)
    g.insert(0, "device_id", pdf["device_id"].iloc[0])
    return g


def majority(pred_df, label):
    """temp1 = groupBy(device, prediction).count(); keep the most frequent prediction per device."""
    w = Window.partitionBy("device_id").orderBy(F.desc("count"), "prediction")
    return pred_df.groupBy("device_id", "prediction").count() \
                  .withColumn("rk", F.row_number().over(w)).where("rk = 1") \
                  .select("device_id", F.col("prediction").alias(label))


if __name__ == "__main__":
    spark = get_spark("ec_web_logs_analysis_2 profiles")
    rep = Report("期末專題 ③：使用者輪廓 + 儀表板（取代 Tableau）")
    logs = spark.read.parquet(f"{BASE}/data").cache()
    T = f"{BASE}/tables"

    with rep.step("device locations (KMeans per device, applyInPandas)"):
        loc = logs.select("device_id", "lat", "lon").groupBy("device_id") \
                  .applyInPandas(kmeans_per_device,
                                 "device_id string, avg_lat double, avg_lon double, lat_lon_count long")
        loc.write.mode("overwrite").parquet(f"{T}/device_inferred_location")
        loc = spark.read.parquet(f"{T}/device_inferred_location")

    with rep.step("infer gender per device"):
        data_prep = VectorAssembler(inputCols=FEATURES, outputCol="features").transform(logs.select("device_id", *FEATURES))
        g_model = RandomForestClassificationModel.load(f"{BASE}/models/model_gender_prediction")
        gender = majority(g_model.transform(data_prep), "inferred_gender")
        gender.write.mode("overwrite").parquet(f"{T}/device_inferred_gender")

    with rep.step("infer age group per device"):
        a_model = RandomForestClassificationModel.load(f"{BASE}/models/model_age_group_prediction")
        labels = StringIndexerModel.load(f"{BASE}/models/age_group_indexer").labels
        to_label = F.udf(lambda i: labels[int(i)])
        age = majority(a_model.transform(data_prep), "idx").select("device_id", to_label("idx").alias("inferred_age_group"))
        age.write.mode("overwrite").parquet(f"{T}/device_inferred_age_group")

    with rep.step("hourly PV / UV"):
        hourly = logs.withColumn("hour", F.hour("timestamp")).groupBy("hour") \
                     .agg(F.count("*").alias("visit_count"), F.countDistinct("device_id").alias("uu_count")) \
                     .orderBy("hour")
        hourly.write.mode("overwrite").parquet(f"{T}/stats_device_hourly_activities")
        hourly = hourly.collect()

    with rep.step("Taipei East District users"):
        from project_etl import get_locations_py
        from pyspark.sql.types import ArrayType, StringType
        get_locations = F.udf(get_locations_py, ArrayType(StringType()))
        east_ids = logs.select("device_id", F.explode(get_locations("quadkey")).alias("region")) \
                       .where("region = '台北東區'").select("device_id").distinct()
        east = east_ids.join(gender, "device_id", "left").join(age, "device_id", "left")
        east.write.mode("overwrite").parquet(f"{T}/eastern_district_device_profiles")
        east = spark.read.parquet(f"{T}/eastern_district_device_profiles").cache()

    g_stats = spark.read.parquet(f"{T}/device_inferred_gender").groupBy("inferred_gender").count() \
                   .orderBy("inferred_gender").collect()
    a_stats = spark.read.parquet(f"{T}/device_inferred_age_group").groupBy("inferred_age_group").count() \
                   .orderBy("inferred_age_group").collect()
    gname = {0.0: "Female (0)", 1.0: "Male (1)"}   # mapping from the course Tableau dashboard
    points = loc.orderBy(F.desc("lat_lon_count")).limit(4000).collect()

    rep.metric("裝置數", logs.select("device_id").distinct().count())
    rep.metric("常去地點（群）", loc.count())
    rep.metric("曾到台北東區的裝置", east.count())
    rep.raw("map_points", [{"lat": p.avg_lat, "lon": p.avg_lon, "label": p.device_id[:8], "size": p.lat_lon_count,
                            "group": p.device_id} for p in points])
    rep.chart("推論性別分布（每台裝置）", "bar", [gname.get(r.inferred_gender, str(r.inferred_gender)) for r in g_stats],
              {"devices": [r["count"] for r in g_stats]})
    rep.chart("推論年齡層分布", "bar", [r.inferred_age_group for r in a_stats], {"devices": [r["count"] for r in a_stats]})
    rep.chart("Hourly PV", "line", [f"{r.hour:02d}" for r in hourly], {"PV": [r.visit_count for r in hourly]})
    rep.chart("Hourly UU", "line", [f"{r.hour:02d}" for r in hourly], {"UU": [r.uu_count for r in hourly]})
    rep.table("台北東區使用者輪廓", east.groupBy(
        F.when(F.col("inferred_gender") == 0, "Female").otherwise("Male").alias("inferred_gender"),
        "inferred_age_group").count().orderBy(F.desc("count")), limit=20)
    rep.note("輸出表格（Parquet，名稱同原本的 MySQL 表）：~/Spark/work/ec_web_logs_analysis/tables/")
    rep.note("性別代碼 0 = Female、1 = Male，依原課程 Tableau 儀表板的對應。資料為模擬資料（1,000 台裝置）。")
    rep.save()
    spark.stop()
