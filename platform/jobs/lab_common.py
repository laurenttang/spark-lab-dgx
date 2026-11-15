"""Shared helpers for Spark Lab jobs.

Every job is a normal PySpark program (runnable with spark-submit, like in the
2020 course). Jobs report what they found through `Report`, which the web
portal renders as metric cards, charts and tables.
"""
import json
import os
import sys
import time

from pyspark.sql import SparkSession

COURSE = "/data/course"      # original course folder (read-only)
WORK = "/work"               # outputs / big data / checkpoints
WEBLOGS = f"{COURSE}/spark_mllib_101/web_logs_analysis/data/"


def weblogs_path(factor, fmt="csv"):
    """Where the (amplified) web logs live: CSV from power_amplify, Parquet from power_to_parquet."""
    if fmt == "parquet":
        return f"{WORK}/bigdata/parquet_x{factor}"
    return WEBLOGS if factor == 1 else f"{WORK}/bigdata/weblogs_x{factor}"


def dir_gb(path, suffix=""):
    return sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(path)
               for f in fs if f.endswith(suffix)) / 1024 ** 3


def get_spark(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def arg(name, default=None, cast=str):
    """Read --name value from argv (portal passes params this way)."""
    flag = "--" + name
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return cast(sys.argv[i + 1])
    return default


class Report:
    def __init__(self, title):
        self.data = {"title": title, "metrics": [], "charts": [], "tables": [], "notes": []}
        self._t0 = time.time()
        self._steps = []

    # --- content -----------------------------------------------------------
    def metric(self, label, value, unit=""):
        self.data["metrics"].append({"label": label, "value": value, "unit": unit})

    def note(self, text):
        self.data["notes"].append(text)
        print("[note]", text, flush=True)

    def table(self, name, df_or_rows, columns=None, limit=50):
        if hasattr(df_or_rows, "limit"):          # Spark DataFrame
            columns = df_or_rows.columns
            rows = [list(r) for r in df_or_rows.limit(limit).collect()]
        else:
            rows = [list(r) for r in df_or_rows][:limit]
        self.data["tables"].append({"name": name, "columns": columns,
                                    "rows": _jsonable(rows)})

    def chart(self, title, kind, labels, series, **opts):
        """kind: bar | line | hbar | scatter | pie ; series: {name: [values]}"""
        self.data["charts"].append({"title": title, "kind": kind,
                                    "labels": _jsonable(labels),
                                    "series": {k: _jsonable(v) for k, v in series.items()},
                                    **opts})

    def raw(self, key, value):
        self.data[key] = _jsonable(value)

    # --- timing ------------------------------------------------------------
    def step(self, name):
        """Context manager that times a stage and prints progress."""
        report = self

        class _Step:
            def __enter__(self):
                print(f"[step] {name} ...", flush=True)
                self.t = time.time()

            def __exit__(self, *exc):
                dt = round(time.time() - self.t, 2)
                report._steps.append({"step": name, "seconds": dt})
                print(f"[step] {name} done in {dt}s", flush=True)

        return _Step()

    def save(self):
        self.data["steps"] = self._steps
        self.data["total_seconds"] = round(time.time() - self._t0, 2)
        path = os.environ.get("LAB_RESULT")
        if path:
            with open(path, "w") as f:
                json.dump(self.data, f, ensure_ascii=False)
        print("[result]", json.dumps({"total_seconds": self.data["total_seconds"]}), flush=True)


def _jsonable(v):
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, float):
        return round(v, 4)
    if v is None or isinstance(v, (int, str, bool)):
        return v
    if hasattr(v, "toArray"):                      # ml Vector
        return [round(float(x), 4) for x in v.toArray()][:10]
    return str(v)
