"""spark101 / access_log_analysis  (lesson 0721)

Parse an Apache access log with regex (match.group), then look up each IP's
country/city with DB-IP City Lite (MaxMind-compatible MMDB, CC BY 4.0).

Key technique from class ("重要技巧 避開 out of memory" / multi-machine with
third-party API): open the GeoIP reader ONCE PER PARTITION with
mapPartitions, not once per record, and never collect the raw data.
"""
import re

from lab_common import COURSE, Report, get_spark

LOG = f"{COURSE}/spark101/access_log_analysis/data/access_log.txt"
MMDB = f"{COURSE}/spark101/access_log_analysis/geoip/dbip-city-lite.mmdb"   # course used MaxMind GeoLite2
PATTERN = re.compile(r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+)[^"]*" (\d{3}) (\S+)')


def parse(line):
    m = PATTERN.match(line)
    if not m:
        return None
    return (m.group(1), m.group(2), m.group(3), m.group(4), int(m.group(5)))


def lookup_partition(ips):
    import geoip2.database                       # imported on the executor
    reader = geoip2.database.Reader(MMDB)
    for ip, hits in ips:
        try:
            r = reader.city(ip)
            yield (ip, hits, r.country.name or "Unknown", r.city.name or "")
        except Exception:
            yield (ip, hits, "Unknown", "")
    reader.close()


if __name__ == "__main__":
    spark = get_spark("spark101 access_log_analysis")
    sc = spark.sparkContext
    rep = Report("網站存取日誌分析 + IP 地理定位")

    with rep.step("regex parse"):
        logs = sc.textFile(LOG, minPartitions=16).map(parse).filter(lambda x: x is not None).cache()
        n = logs.count()

    with rep.step("top URLs / status codes"):
        top_urls = logs.map(lambda r: (r[3], 1)).reduceByKey(lambda a, b: a + b) \
                       .sortBy(lambda p: -p[1]).take(15)
        status = sorted(logs.map(lambda r: (r[4], 1)).reduceByKey(lambda a, b: a + b).collect())

    with rep.step("GeoIP lookup with mapPartitions"):
        ip_hits = logs.map(lambda r: (r[0], 1)).reduceByKey(lambda a, b: a + b)
        geo = ip_hits.mapPartitions(lookup_partition).cache()
        by_country = geo.map(lambda g: (g[2], g[1])).reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda p: -p[1]).take(15)
        top_ips = geo.sortBy(lambda g: -g[1]).take(15)

    rep.metric("日誌筆數", n)
    rep.metric("不同 IP 數", ip_hits.count())
    rep.metric("國家數", geo.map(lambda g: g[2]).distinct().count())
    rep.chart("各國請求數 Top 15", "hbar", [c for c, _ in by_country], {"requests": [h for _, h in by_country]})
    rep.chart("HTTP 狀態碼", "bar", [str(s) for s, _ in status], {"count": [c for _, c in status]})
    rep.table("Top URLs", top_urls, ["url", "hits"])
    rep.table("Top IPs (GeoIP)", top_ips, ["ip", "hits", "country", "city"])
    rep.note("access log 是模擬資料（IP 隨機產生），定位用 DB-IP City Lite（IP Geolocation by DB-IP, CC BY 4.0）。")
    rep.save()
    spark.stop()
