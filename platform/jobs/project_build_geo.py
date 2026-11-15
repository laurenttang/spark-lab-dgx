"""期末專題 / step 0: rebuild geo_data/*.csv  (region_name, quadkey)

The original geo_data/ folder (region_qk_tw_adm.csv, region_qk_tw_major_air_ports.csv,
region_qk_taipei_eastern_district.csv) was lost. We regenerate it the same way the
lesson describes ("Quadkey Tree", 0818): cover each region polygon with the smallest
set of Bing-Maps quadkey tiles. A tile fully inside the polygon is emitted as one
short quadkey (its prefix covers every child); edge tiles are split further.

Sources: Taiwan county boundaries from g0v/twgeojson (twCounty2010), airports and
Taipei East District drawn as small circles / a box around well-known coordinates.
Runs as plain Python (shapely + mercantile), no Spark needed.
"""
import csv
import json
import os

import mercantile
from shapely.geometry import Point, box, shape
from shapely.prepared import prep

from lab_common import WORK, Report

SRC = f"{WORK}/geo_src/twCounty2010.geo.json"
OUT = f"{WORK}/ec_web_logs_analysis/geo_data"

AIRPORTS = {  # name: (lat, lon, radius_km)
    "桃園國際機場": (25.0777, 121.2328, 2.5), "台北松山機場": (25.0694, 121.5525, 1.2),
    "高雄國際機場": (22.5771, 120.3500, 1.3), "台中清泉崗機場": (24.2647, 120.6208, 1.5),
    "台南機場": (22.9504, 120.2057, 1.2), "嘉義機場": (23.4618, 120.3928, 1.0),
    "花蓮機場": (24.0231, 121.6162, 1.0), "台東豐年機場": (22.7550, 121.1018, 1.0),
    "澎湖馬公機場": (23.5687, 119.6280, 1.2), "金門尚義機場": (24.4279, 118.3592, 1.0),
}
TAIPEI_EAST = {"台北東區": box(121.5435, 25.0365, 121.5575, 25.0450)}   # 復興南路~光復南路, 仁愛路~市民大道


def cover(geom, min_z, max_z):
    g = prep(geom)
    keys = []

    def visit(t):
        tb = box(*mercantile.bounds(t))
        if not g.intersects(tb):
            return
        if g.contains(tb):
            keys.append(mercantile.quadkey(t))
        elif t.z >= max_z:
            if geom.contains(tb.centroid):
                keys.append(mercantile.quadkey(t))
        else:
            for c in mercantile.children(t):
                visit(c)

    for t in mercantile.tiles(*geom.bounds, zooms=min_z):
        visit(t)
    return keys


def write(name, regions, min_z, max_z):
    path = f"{OUT}/{name}"
    n = 0
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["region_name", "quadkey"])
        for region, geom in regions.items():
            for qk in cover(geom, min_z, max_z):
                w.writerow([region, qk])
                n += 1
    return n


if __name__ == "__main__":
    rep = Report("期末專題 0：重建 geo_data 區域 quadkey 表")
    os.makedirs(OUT, exist_ok=True)
    with rep.step("counties -> quadkeys (z8..z15)"):
        feats = json.load(open(SRC))["features"]
        counties = {f["properties"]["COUNTYNAME"].replace("桃園縣", "桃園市"): shape(f["geometry"]).buffer(0)
                    for f in feats}
        n_adm = write("region_qk_tw_adm.csv", counties, 8, 15)
    with rep.step("airports -> quadkeys (z12..z17)"):
        ap = {k: Point(lon, lat).buffer(r / 111.0) for k, (lat, lon, r) in AIRPORTS.items()}
        n_ap = write("region_qk_tw_major_air_ports.csv", ap, 12, 17)
    with rep.step("Taipei East District -> quadkeys (z14..z19)"):
        n_east = write("region_qk_taipei_eastern_district.csv", TAIPEI_EAST, 14, 19)
    rep.metric("縣市", len(counties))
    rep.metric("縣市 quadkeys", n_adm)
    rep.metric("機場 quadkeys", n_ap)
    rep.metric("東區 quadkeys", n_east)
    rep.note("縣市邊界：g0v/twgeojson twCounty2010；桃園縣已更名桃園市。機場與東區範圍為依座標手繪的近似範圍。")
    rep.save()
