"""Prepare every dataset Spark Lab needs, inside the spark-lab container.

    python /tools/prepare_data.py            # called by platform/bootstrap.sh

Three kinds of data (see NOTICE.md for sources and licenses):
  * committed to git   : public-domain / openly licensed files (War and Peace, NOAA sample,
                         London crime [OGL v3], Hadoop-book join sample [Apache-2.0])
  * downloaded here    : datasets whose licenses do not allow redistribution in this repo
                         (MovieLens, Sentiment140, SMS Spam Collection, Titanic, DB-IP Lite)
  * generated here     : synthetic stand-ins for course data that cannot be published
                         (e-commerce web logs, Apache access log, Hive page views, stocks)

Every step is idempotent: existing outputs are kept, so re-running is cheap.
Generation is seeded, so every clone gets the same synthetic data.
"""
import csv
import datetime as dt
import gzip
import io
import json
import os
import random
import shutil
import sys
import time
import urllib.error
import urllib.request
import uuid
import zipfile

import numpy as np

D = "/data/course"
WORK = "/work"
UA = {"User-Agent": "spark-lab-dgx-setup/1.0"}


def log(msg):
    print(f"[data] {msg}", flush=True)


def done(path):
    return os.path.exists(path) and os.path.getsize(path) > 0


def clean(text):
    """Spark's CSV reader escapes with a backslash, not RFC 4180's doubled quote, so a '"' inside
    a field written by csv.writer ("") splits the row. Keep free text free of '"' and newlines."""
    return text.replace('"', "'").replace("\r", " ").replace("\n", " ")


def fetch(url, tries=6):
    """Download to work/downloads/ (cached), resuming with HTTP Range after a stall.

    Some dataset servers are slow (~150 KB/s) and drop connections mid-file, so a
    plain urlopen().read() fails on a fresh machine. Returns the file's bytes.
    """
    cache = out(f"{WORK}/downloads/{url.rstrip('/').split('/')[-1]}")
    if done(cache) and not os.path.exists(cache + ".part"):
        return open(cache, "rb").read()
    part = cache + ".part"
    for attempt in range(1, tries + 1):
        have = os.path.getsize(part) if os.path.exists(part) else 0
        headers = dict(UA, **({"Range": f"bytes={have}-"} if have else {}))
        log(f"download {url}" + (f" (resume at {have / 1e6:.0f} MB, try {attempt})" if have else ""))
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as r:
                if have and r.status != 206:          # server ignored Range: start over
                    have = 0
                total = have + int(r.headers.get("Content-Length") or 0)
                with open(part, "ab" if have else "wb") as f:
                    next_mark = have + 10_000_000
                    while chunk := r.read(1 << 20):
                        f.write(chunk)
                        have += len(chunk)
                        if have >= next_mark and total:
                            log(f"  {have / 1e6:.0f} / {total / 1e6:.0f} MB")
                            next_mark += 10_000_000
            os.replace(part, cache)
            return open(cache, "rb").read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404, 410):              # not there (e.g. DB-IP's next month): no retry
                raise
            log(f"  interrupted: {e!r}")
            time.sleep(min(30, 5 * attempt))
        except Exception as e:                        # noqa: BLE001  (timeouts, resets)
            log(f"  interrupted: {e!r}")
            time.sleep(min(30, 5 * attempt))
    raise RuntimeError(f"download failed after {tries} tries: {url}")


def out(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


# --------------------------------------------------------------------------- committed files
def unpack_committed():
    src = f"{D}/spark_sql_101/crime/data/london_crime_by_lsoa.csv"
    if not done(src):
        log("gunzip london_crime_by_lsoa.csv.gz")
        with gzip.open(src + ".gz", "rb") as fi, open(src, "wb") as fo:
            shutil.copyfileobj(fi, fo)


# --------------------------------------------------------------------------- downloads
def taiwan_counties():
    path = out(f"{WORK}/geo_src/twCounty2010.geo.json")
    if not done(path):
        data = fetch("https://cdn.jsdelivr.net/gh/g0v/twgeojson@master/json/twCounty2010.geo.json")
        open(path, "wb").write(data)
    return path


def movielens():
    ratings = out(f"{D}/spark101/movies_similarity/data/movie_ratings/movie_ratings")
    movies = out(f"{D}/spark101/movies_similarity/data/movies/movies")
    if not (done(ratings) and done(movies)):
        z = zipfile.ZipFile(io.BytesIO(fetch("https://files.grouplens.org/datasets/movielens/ml-100k.zip")))
        open(ratings, "wb").write(z.read("ml-100k/u.data"))                 # user \t movie \t rating \t ts
        with open(movies, "w", newline="", encoding="utf-8") as f:           # "id,title" like the course file
            w = csv.writer(f)
            for line in z.read("ml-100k/u.item").decode("latin-1").splitlines():
                p = line.split("|")
                w.writerow([p[0], clean(p[1])])
    small = out(f"{D}/spark_mllib_101/movies/ratings.csv")
    if not (done(small) and done(f"{D}/spark_mllib_101/movies/movies.csv")):
        z = zipfile.ZipFile(io.BytesIO(fetch("https://files.grouplens.org/datasets/movielens/ml-latest-small.zip")))
        for name in ("ratings.csv", "movies.csv"):
            open(f"{D}/spark_mllib_101/movies/{name}", "wb").write(z.read(f"ml-latest-small/{name}"))


def sentiment140():
    full = out(f"{D}/spark_streaming_101/simple_sentiment_analysis_demo/data/messages.csv")
    sample = out(f"{D}/spark_sql_101/messages/data/user_messages.csv")
    if done(full) and done(sample):
        return
    z = zipfile.ZipFile(io.BytesIO(fetch("https://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip")))
    raw = z.read("training.1600000.processed.noemoticon.csv")
    open(full, "wb").write(raw)                                              # latin-1, 6 columns, no header
    rows = csv.reader(io.StringIO(raw.decode("latin-1")))
    with open(sample, "w", newline="", encoding="utf-8") as f:              # first 10,000 tweets, like the course
        w = csv.writer(f)
        w.writerow(["timestamp", "user", "message"])
        for i, r in enumerate(rows):
            if i >= 10000:
                break
            w.writerow([r[2], r[4], clean(r[5])])


def sms_spam():
    path = out(f"{D}/spark_mllib_101/spam_detection/data/sms_messages_with_labels.csv")
    if done(path):
        return
    z = zipfile.ZipFile(io.BytesIO(fetch("https://archive.ics.uci.edu/static/public/228/sms+spam+collection.zip")))
    text = z.read("SMSSpamCollection").decode("utf-8", errors="replace")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["message", "label"])
        for line in text.splitlines():
            label, _, msg = line.partition("\t")
            if msg:
                w.writerow([clean(msg), label])


def titanic():
    path = out(f"{D}/spark_mllib_101/titanic/titanic.csv")
    if not done(path):
        open(path, "wb").write(fetch("https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"))


def dbip():
    """DB-IP City Lite (CC BY 4.0): same MMDB format as MaxMind GeoLite2, no account needed."""
    path = out(f"{D}/spark101/access_log_analysis/geoip/dbip-city-lite.mmdb")
    if done(path):
        return
    today = dt.date.today().replace(day=1)
    for back in range(4):                                                    # this month, else the last few
        month = (today - dt.timedelta(days=28 * back)).strftime("%Y-%m")
        try:
            data = fetch(f"https://download.db-ip.com/free/dbip-city-lite-{month}.mmdb.gz")
            open(path, "wb").write(gzip.decompress(data))
            return
        except Exception as e:                                               # noqa: BLE001
            log(f"  {month}: {e}")
    raise SystemExit("could not download DB-IP City Lite")


# --------------------------------------------------------------------------- synthetic: e-commerce logs
COUNTY_W = {   # where devices "live"; skewed to the big cities like the course data (Taipei ~46 %)
    "台北市": .25, "新北市": .21, "台中市": .16, "桃園縣": .08, "台南市": .05, "高雄市": .06, "新竹市": .04,
    "新竹縣": .02, "基隆市": .015, "宜蘭縣": .01, "花蓮縣": .01, "屏東縣": .01, "南投縣": .01, "彰化縣": .015,
    "苗栗縣": .01, "雲林縣": .005, "嘉義市": .005, "嘉義縣": .005, "台東縣": .005, "澎湖縣": .005,
}
CITY_LABEL = {   # county -> (city_en, city_ch) as an IP-geolocation service would label it
    "台北市": ("Taipei", "台北市"), "新北市": ("New Taipei", "新北市"), "台中市": ("Taichung", "台中市"),
    "桃園縣": ("Taoyuan District", "桃園區"), "台南市": ("Tainan City", "台南市"), "高雄市": ("Kaohsiung", "高雄市"),
    "新竹市": ("Hsinchu", "新竹市"), "新竹縣": ("Zhubei", "竹北市"), "基隆市": ("Keelung", "基隆市"),
    "宜蘭縣": ("Yilan", "宜蘭市"), "花蓮縣": ("Hualien City", "花蓮市"), "屏東縣": ("Pingtung City", "屏東市"),
    "南投縣": ("Nantou City", "南投市"), "彰化縣": ("Changhua", "彰化市"), "苗栗縣": ("Miaoli", "苗栗市"),
    "雲林縣": ("Douliu", "斗六市"), "嘉義市": ("Chiayi City", "嘉義市"), "嘉義縣": ("Taibao", "太保市"),
    "台東縣": ("Taitung City", "台東市"), "澎湖縣": ("Magong", "馬公"),
}
EAST_DISTRICT = (121.5435, 25.0365, 121.5575, 25.0450)          # lon/lat box used by project_build_geo.py
AIRPORTS = [(25.0777, 121.2328), (25.0697, 121.5525), (22.5771, 120.3500), (24.2647, 120.6208)]
FOREIGN = [("Japan", "日本", "Tokyo", "東京", 35.68, 139.76), ("United States", "美國", "San Francisco", "舊金山", 37.77, -122.42)]
AGE_GENDER = [("21-35", 1, 311), ("36-50", 0, 261), ("over 50", 1, 176), ("under 20", 0, 252)]   # devices
HOUR_W = [.0435, .0416, .0393, .037, .0363, .0361, .0377, .0398, .0404, .0403, .0412, .0418, .0428, .0422,
          .042, .0422, .0428, .0434, .0441, .0445, .0449, .0452, .0456, .0454]
WEEKDAY_W = [.1308, .1309, .15, .1591, .1599, .1391, .1302]
TW_PREFIXES = ["1.160", "1.161", "1.170", "36.224", "36.226", "36.231", "42.72", "49.158", "59.115", "61.216",
               "111.240", "111.248", "114.24", "114.32", "114.136", "118.160", "118.166", "122.116", "125.224",
               "180.176", "210.61", "211.20", "218.161", "220.128", "220.135"]


# Share of each 20-category block (1-20, 21-40, ... 141-160) by age group, and the joint
# (device_type, connection_type) mix by age group: the signals the course's age and gender
# models learn. Taken from aggregate statistics of the course data.
CATEGORY_BLOCKS = {
    "under 20": [.778, .074, .074, .074, 0, 0, 0, 0],
    "36-50":    [.074, .074, .778, .074, 0, 0, 0, 0],
    "21-35":    [.037, .037, .037, .037, .037, .741, .037, .037],
    "over 50":  [.037, .037, .037, .037, .037, .037, .037, .741],
}
DEVICE_CONN = {   # P(device_type, connection_type) for (1,1), (1,2), (2,1), (2,2)
    "21-35": [.327, .374, .140, .160], "36-50": [.160, .141, .372, .327],
    "over 50": [.186, .513, .080, .221], "under 20": [.080, .221, .188, .512],
}


def category_probs(age):
    p = np.zeros(161)
    for b, share in enumerate(CATEGORY_BLOCKS[age]):
        p[1 + 20 * b: 21 + 20 * b] = share / 20
    return p / p.sum()


def county_sampler(geo_path, rng):
    from shapely.geometry import Point, shape
    from shapely.prepared import prep
    shapes = {}
    for f in json.load(open(geo_path))["features"]:
        g = shape(f["geometry"])
        shapes[f["properties"]["COUNTYNAME"]] = (prep(g), g.bounds)

    def sample(county):
        pg, (x0, y0, x1, y1) = shapes[county]
        while True:
            lon, lat = rng.uniform(x0, x1), rng.uniform(y0, y1)
            if pg.contains(Point(lon, lat)):
                return lat, lon
    return sample


def ec_web_logs():
    target = f"{D}/spark_mllib_101/web_logs_analysis/data"
    if done(f"{target}/logs_201905_7.csv"):
        return
    import pandas as pd
    log("generate synthetic e-commerce web logs (1,000 devices, ~1.28M rows) ...")
    rng = np.random.default_rng(2019)
    prng = random.Random(2019)
    sample_point = county_sampler(taiwan_counties(), prng)
    counties = list(COUNTY_W)
    cw = np.array([COUNTY_W[c] for c in counties]); cw /= cw.sum()
    days = [dt.date(2019, 5, 1) + dt.timedelta(d) for d in range(31)]
    dw = np.array([WEEKDAY_W[d.weekday()] for d in days]); dw /= dw.sum()
    hw = np.array(HOUR_W) / sum(HOUR_W)
    epoch = dt.datetime(1970, 1, 1)
    day_secs = np.array([(dt.datetime(d.year, d.month, d.day) - epoch).total_seconds() for d in days])

    frames = []
    for age, gender, n_dev in AGE_GENDER:
        cat_p = category_probs(age)
        dc_p = np.array(DEVICE_CONN[age]) / sum(DEVICE_CONN[age])
        for _ in range(n_dev):
            n = int(1000 + rng.lognormal(np.log(190), 0.9))
            home = counties[rng.choice(len(counties), p=cw)]
            places = [(sample_point(home), .55, home)]
            work = home if rng.random() < .7 else "台北市"
            places.append((sample_point(work), .30, work))
            for _ in range(rng.integers(0, 4)):                       # occasional trips
                c = counties[rng.choice(len(counties), p=cw)]
                places.append((sample_point(c), .04, c))
            if rng.random() < .20:                                    # shops in Taipei's East District
                x0, y0, x1, y1 = EAST_DISTRICT
                places.append(((rng.uniform(y0, y1), rng.uniform(x0, x1)), .05, "台北市"))
            if rng.random() < .05:                                    # flies now and then
                places.append((AIRPORTS[rng.integers(len(AIRPORTS))], .02, None))
            w = np.array([p[1] for p in places]); w /= w.sum()
            pick = rng.choice(len(places), size=n, p=w)
            lat = np.array([places[i][0][0] for i in pick]) + rng.normal(0, .003, n)
            lon = np.array([places[i][0][1] for i in pick]) + rng.normal(0, .003, n)
            label = [places[i][2] or home for i in pick]
            secs = (day_secs[rng.choice(31, size=n, p=dw)] + rng.choice(24, size=n, p=hw) * 3600
                    + rng.integers(0, 3600, n))
            ips = [f"{prng.choice(TW_PREFIXES)}.{prng.randint(0, 255)}.{prng.randint(1, 254)}"
                   for _ in range(rng.integers(1, 4))]
            frames.append(pd.DataFrame({
                "device_id": str(uuid.UUID(int=prng.getrandbits(128))),
                "ts": secs,
                "member_id": "".join(prng.choice("0123456789") for _ in range(87)),
                "product_category_id": rng.choice(161, size=n, p=cat_p),
                "ip": [ips[i] for i in rng.integers(0, len(ips), n)],
                "lat": lat.round(14), "lon": lon.round(14),
                "county": label,
                "dc": rng.choice(4, size=n, p=dc_p),
                "age_group": age, "gender": gender,
            }))
    df = pd.concat(frames, ignore_index=True)
    df["device_type"] = np.where(df.dc < 2, 1, 2)
    df["connection_type"] = np.where(df.dc % 2 == 0, 1, 2)
    df["country_en"], df["country_ch"] = "Taiwan", "台灣"
    df["city_en"] = df.county.map(lambda c: CITY_LABEL[c][0])
    df["city_ch"] = df.county.map(lambda c: CITY_LABEL[c][1])
    foreign = rng.random(len(df)) < .001                              # a few roaming visits abroad
    for i, (cen, cch, ten, tch, la, lo) in enumerate(FOREIGN):
        m = foreign & (rng.integers(0, len(FOREIGN), len(df)) == i)
        df.loc[m, ["country_en", "country_ch", "city_en", "city_ch"]] = [cen, cch, ten, tch]
        df.loc[m, "lat"] = la + rng.normal(0, .05, m.sum())
        df.loc[m, "lon"] = lo + rng.normal(0, .05, m.sum())
    df.loc[rng.random(len(df)) < .036, "city_en"] = None              # geolocation often has no city
    df.loc[rng.random(len(df)) < .21, "city_ch"] = None
    local = pd.to_datetime(df.ts, unit="s")
    df["timestamp"] = local.dt.strftime("%Y-%m-%dT%H:%M:%S.000+08:00")
    cols = ["device_id", "timestamp", "member_id", "product_category_id", "ip", "lat", "lon", "country_en",
            "country_ch", "city_en", "city_ch", "device_type", "connection_type", "age_group", "gender"]
    df = df[cols].sample(frac=1, random_state=7).reset_index(drop=True)
    os.makedirs(target, exist_ok=True)
    bounds = np.linspace(0, len(df), 9).astype(int)                   # 8 files, like the course data
    for i in range(8):
        df.iloc[bounds[i]:bounds[i + 1]].to_csv(f"{target}/logs_201905_{i}.csv", index=False)
    log(f"  {len(df):,} rows, {df.device_id.nunique()} devices")


# --------------------------------------------------------------------------- synthetic: small logs
def access_log():
    path = out(f"{D}/spark101/access_log_analysis/data/access_log.txt")
    if done(path):
        return
    log("generate synthetic Apache access log ...")
    r = random.Random(2015)

    def public_ip():
        while True:
            a = r.randint(1, 223)
            if a not in (10, 100, 127, 169, 172, 192, 198, 203):
                return f"{a}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"
    visitors = [public_ip() for _ in range(3000)]
    weights = [1 / (i + 1) ** 0.9 for i in range(len(visitors))]
    pages = ["/", "/blog/", "/robots.txt", "/feed/", "/about/", "/contact/", "/wp-login.php", "/sitemap.xml",
             "/favicon.ico"] + [f"/blog/post-{i}/" for i in range(1, 120)]
    agents = ["Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
              "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0 Safari/537.36",
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/601.2.7 (KHTML, like Gecko) Version/9.0.1 Safari/601.2.7",
              "Mozilla/5.0 (Windows NT 6.1; rv:42.0) Gecko/20100101 Firefox/42.0",
              "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13B143"]
    t = dt.datetime(2015, 11, 29, 3, 50, 5)
    with open(path, "w") as f:
        for _ in range(76000):
            t += dt.timedelta(seconds=r.expovariate(1 / 8))
            page = r.choice(pages)
            status = r.choices([200, 304, 301, 404, 500], [80, 8, 5, 6, 1])[0]
            size = r.randint(200, 40000) if status == 200 else (0 if status == 304 else r.randint(150, 600))
            method = "POST" if page == "/wp-login.php" and r.random() < .5 else "GET"
            ref = r.choice(["-", "-", "https://www.google.com/", "https://www.bing.com/", "http://t.co/x"])
            f.write(f'{r.choices(visitors, weights)[0]} - - [{t.strftime("%d/%b/%Y:%H:%M:%S")} +0000] '
                    f'"{method} {page} HTTP/1.1" {status} {size} "{ref}" "{r.choice(agents)}"\n')


def hive_samples():
    pv = f"{D}/hive101/page_views/data"
    if not os.path.isdir(pv) or not os.listdir(pv):
        log("generate synthetic Hive page_views logs ...")
        r = random.Random(2013)
        os.makedirs(pv, exist_ok=True)
        pages = ["Sporting", "ShoppingCart", "Electronics", "Books", "Music", "Checkout", "Home", "Garden"]
        refs = ["Google.com", "Yahoo.com", "Bing.com", "Facebook.com", "Twitter.com", "Direct"]
        oses = [("Android", ["2", "4", "5"], ["HTC", "Samsung", "Sony"]), ("iOS", ["6", "7"], ["iPhone", "iPad"]),
                ("Windows", ["7", "8"], ["Chrome", "Firefox", "IE"]), ("MacOS", ["10"], ["Safari", "Chrome"])]
        for start, n in [(dt.datetime(2012, 6, 13, 16, 11, 17), 1500), (dt.datetime(2013, 7, 11, 15, 53, 54), 10000),
                         (dt.datetime(2013, 7, 20, 16, 22, 56), 6000), (dt.datetime(2013, 8, 3, 15, 59, 0), 10000),
                         (dt.datetime(2013, 8, 5, 16, 21, 0), 17000), (dt.datetime(2013, 8, 11, 16, 13, 6), 10000)]:
            t = start
            name = f"log_{start.year}{start.month}{start.day:02d}_{start.hour}{start.minute}{start.second}.log"
            with open(f"{pv}/{name}", "w") as f:
                for _ in range(n):
                    t += dt.timedelta(seconds=r.choice([0, 0, 1, 2, 5]))
                    os_name, vers, agents = r.choice(oses)
                    ip = ".".join(str(r.randint(1, 254)) for _ in range(4))
                    f.write(f"{t.strftime('%m/%d/%Y %H:%M:%S')}\t{r.randint(1, 100)}\t{ip}\t{r.choice(pages)}\t"
                            f"{r.choice(refs)}\t{os_name}\t{r.choice(vers)}\t{r.choice(agents)}\n")
    stocks = out(f"{D}/hive101/stocks/data/stocks.txt")
    if not done(stocks):
        log("generate synthetic stock prices ...")
        r = random.Random(2009)
        levels = {"AAPL": 90, "GOOG": 300, "IBM": 85, "INTC": 20, "CSCO": 18, "MSFT": 26}
        with open(stocks, "w") as f:
            for sym, base in levels.items():
                for year in range(2009, 2001, -1):
                    o = base * r.uniform(.6, 1.8)
                    hi, lo = o * r.uniform(1, 1.05), o * r.uniform(.95, 1)
                    c = r.uniform(lo, hi)
                    f.write(f"{sym},{year}-01-02,{o:.2f},{hi:.2f},{lo:.2f},{c:.2f},{r.randint(5_000_000, 90_000_000)},{c:.2f}\n")


STEPS = [unpack_committed, taiwan_counties, movielens, sentiment140, sms_spam, titanic, dbip,
         ec_web_logs, access_log, hive_samples]

if __name__ == "__main__":
    only = set(sys.argv[1:])
    for step in STEPS:
        if not only or step.__name__ in only:
            step()
    log("all datasets ready")
