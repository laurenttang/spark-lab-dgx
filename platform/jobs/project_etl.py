"""期末專題 / ec_web_logs_analysis_1  steps 2-3  (lessons 0818-0819)

etl_generate_quadkey.py  : raw CSV -> add level-20 quadkey from (lat, lon) -> Parquet
location_analysis_trial_runs.py : explode(get_locations(quadkey)) with the
    QuadkeyTemplateDB prefix tree -> events / users per county, airport, Taipei East.

The class used `from lib_quadkey import from_geo` (a zipped quadkey lib shipped with
--py-files); here mercantile computes the same Bing-Maps quadkey.
"""
import mercantile
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from lab_common import COURSE, WORK, Report, get_spark

BASE = f"{WORK}/ec_web_logs_analysis"
GEO = [f"{BASE}/geo_data/region_qk_tw_adm.csv",
       f"{BASE}/geo_data/region_qk_tw_major_air_ports.csv",
       f"{BASE}/geo_data/region_qk_taipei_eastern_district.csv"]


def generate_quadkey_py(lat, lon):
    if lat is None or lon is None:
        return None
    return mercantile.quadkey(mercantile.tile(lon, lat, 20))


qkDB = None


def get_locations_py(quadkey):
    global qkDB                                   # built once per Python worker
    if qkDB is None:
        from lib_quadkey_template_db import QuadkeyTemplateDB
        qkDB = QuadkeyTemplateDB(GEO)
    found = qkDB.lookup_regions(quadkey)
    return found if found else ["NotFound"]


if __name__ == "__main__":
    spark = get_spark("ec_web_logs_analysis_1 etl")
    spark.sparkContext.addPyFile("/opt/lab/jobs/lib_quadkey_template_db.py")   # = --py-files
    rep = Report("期末專題 ①：Quadkey ETL + 地點分析")

    with rep.step("read raw CSV (explicit schema)"):
        raw = spark.read.csv(f"{COURSE}/spark_mllib_101/web_logs_analysis/data/", header=True,
                             schema="device_id string, timestamp timestamp, member_id string, "
                                    "product_category_id int, ip string, lat float, lon float, "
                                    "country_en string, country_ch string, city_en string, city_ch string, "
                                    "device_type int, connect_type int, age_group string, gender int")

    with rep.step("ETL: add quadkey -> Parquet"):
        generate_quadkey = F.udf(generate_quadkey_py, StringType())
        data = raw.withColumn("quadkey", generate_quadkey("lat", "lon"))
        data.write.mode("overwrite").parquet(f"{BASE}/data")

    with rep.step("location analysis: explode(get_locations(quadkey))"):
        get_locations = F.udf(get_locations_py, ArrayType(StringType()))
        logs = spark.read.parquet(f"{BASE}/data")
        located = logs.select("device_id", F.explode(get_locations("quadkey")).alias("region")).cache()
        regions = located.groupBy("region").agg(F.count("*").alias("events"),
                                               F.countDistinct("device_id").alias("devices")) \
                         .orderBy(F.desc("events")).collect()

    airports = {"桃園國際機場", "台北松山機場", "高雄國際機場", "台中清泉崗機場", "台南機場", "嘉義機場",
                "花蓮機場", "台東豐年機場", "澎湖馬公機場", "金門尚義機場"}
    counties = [r for r in regions if r.region not in airports and r.region not in ("台北東區", "NotFound")]
    rep.metric("Log 筆數", logs.count())
    rep.metric("Parquet 位置", "/work/ec_web_logs_analysis/data")
    rep.metric("落在台灣縣市內", f"{sum(r.events for r in counties) / logs.count():.1%}")
    rep.metric("台北東區 裝置數", next((r.devices for r in regions if r.region == "台北東區"), 0))
    rep.chart("各縣市事件數", "hbar", [r.region for r in counties], {"events": [r.events for r in counties]})
    rep.chart("各縣市使用者（裝置）數", "hbar", [r.region for r in counties], {"devices": [r.devices for r in counties]})
    rep.table("機場周邊活動", [(r.region, r.events, r.devices) for r in regions if r.region in airports],
              ["機場", "events", "devices"])
    rep.table("quadkey 範例", logs.select("device_id", "lat", "lon", "quadkey", "city_ch"), limit=8)
    rep.save()
    spark.stop()
