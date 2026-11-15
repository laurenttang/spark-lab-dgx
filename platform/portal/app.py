"""Spark Lab web portal: run course demos with one click, no terminal.

Jobs run one at a time through spark-submit against the standalone
cluster; their Report JSON is rendered by static/index.html.
"""
import json
import os
import queue
import shutil
import subprocess
import threading
import time
import urllib.request
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse

from portal.catalog import JOBS, SECTIONS

JOBS_DIR = Path("/opt/lab/jobs")
WORK_DIR = Path("/work")
RESULTS = Path("/work/results")
LOGS = Path("/work/logs")
BIG = Path("/work/bigdata")
STATIC = Path(__file__).parent / "static"
RESULTS.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

RAPIDS_JAR = os.environ.get("RAPIDS_JAR", "/opt/rapids/rapids-4-spark_2.13-26.10.0-cuda13-arm64.jar")
RAPIDS_CONF = {                   # see README §6 for why each one is needed on GB10
    "spark.plugins": "com.nvidia.spark.SQLPlugin",
    "spark.rapids.sql.explain": "NOT_ON_GPU",          # log every operator that falls back to CPU
    "spark.rapids.sql.concurrentGpuTasks": "2",
    "spark.rapids.memory.gpu.allocFraction": "0.5",     # GB10 shares RAM: "free" is small, so take
    "spark.rapids.memory.gpu.minAllocFraction": "0.05", # half of it and relax the 25%-of-total minimum
    "spark.rapids.memory.gpu.maxAllocFraction": "0.5",
    "spark.rapids.memory.pinnedPool.size": "4g",
    "spark.rapids.memory.host.offHeapLimit.size": "6g",
    "spark.sql.cache.serializer": "com.nvidia.spark.ParquetCachedBatchSerializer",
    "spark.eventLog.enabled": "true",
}
app = FastAPI(title="Spark Lab")
PROCS = {}                        # run_id -> Popen (running only)
RUNS = {}                         # run_id -> dict (in memory, newest kept)
_queue = queue.Queue()
_lock = threading.Lock()


def _spark_submit(run):
    job = JOBS[run["job"]]
    if job.get("engine") == "python":           # plain single-process baseline (no Spark)
        cmd = ["python", str(JOBS_DIR / job["file"])]
    elif job.get("engine") == "gpu":            # RAPIDS on the GB10: local mode inside the portal (has the GPU)
        cmd = ["spark-submit", "--name", job["title"], "--master", f"local[{run['cores']}]",
               "--driver-memory", os.environ.get("GPU_DRIVER_MEMORY", "8g"),
               "--jars", RAPIDS_JAR, *[a for k, v in RAPIDS_CONF.items() for a in ("--conf", f"{k}={v}")],
               str(JOBS_DIR / job["file"])]
    else:
        cmd = ["spark-submit",
               "--name", f"{job['title']}",
               "--driver-memory", os.environ.get("DRIVER_MEMORY", "4g"),
               "--conf", "spark.driver.host=portal",
               "--conf", f"spark.cores.max={run['cores']}",
               str(JOBS_DIR / job["file"])]
    for k, v in run["params"].items():
        if v not in ("", None):
            cmd += [f"--{k}", str(v)]
    env = dict(os.environ, LAB_RESULT=str(RESULTS / f"{run['id']}.json"))
    log_path = LOGS / f"{run['id']}.log"
    run.update(status="running", started=time.time(), cmd=" ".join(cmd))
    with open(log_path, "w") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, cwd="/opt/lab")
        PROCS[run["id"]] = proc
        rc = proc.wait()
        PROCS.pop(run["id"], None)
    run["finished"] = time.time()
    run["seconds"] = round(run["finished"] - run["started"], 1)
    if run.get("status") != "cancelled":
        run["status"] = "done" if rc == 0 and (RESULTS / f"{run['id']}.json").exists() else "failed"
    (RESULTS / f"{run['id']}.meta").write_text(json.dumps(run, ensure_ascii=False))


def _load_history():
    """Runs survive portal restarts: each finished run leaves a small .meta file."""
    for m in sorted(RESULTS.glob("*.meta"))[-200:]:
        try:
            r = json.loads(m.read_text())
            RUNS[r["id"]] = r
        except (json.JSONDecodeError, KeyError):
            pass


KEEP_RUNS = int(os.environ.get("KEEP_RUNS", "300"))


def _prune_runs():
    """The host never reboots: keep only the newest KEEP_RUNS runs on disk and in memory."""
    metas = sorted(RESULTS.glob("*.meta"))
    for m in metas[:-KEEP_RUNS]:
        rid = m.stem
        for f in (m, RESULTS / f"{rid}.json", LOGS / f"{rid}.log"):
            f.unlink(missing_ok=True)
        RUNS.pop(rid, None)


def _worker():
    while True:
        run = _queue.get()
        if run["status"] == "cancelled":
            continue
        try:
            _spark_submit(run)
            _prune_runs()
        except Exception as e:  # keep the queue alive
            run.update(status="failed", error=str(e))


_prune_runs()
_load_history()
threading.Thread(target=_worker, daemon=True).start()


def _cluster():
    try:
        with urllib.request.urlopen("http://spark-master:8080/json/", timeout=2) as r:
            return json.load(r)
    except Exception as e:
        return {"error": str(e)}


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/catalog")
def catalog():
    return {"sections": [dict(id=s, title=t, desc=d) for s, t, d in SECTIONS],
            "jobs": {k: {kk: vv for kk, vv in v.items() if kk != "file"} for k, v in JOBS.items()},
            "streams": {k: dict(title=v["title"]) for k, v in STREAM_DEFS.items()}}


@app.get("/api/cluster")
def cluster():
    c = _cluster()
    if "error" in c:
        return c
    return {
        "status": c.get("status"),
        "cores": c.get("cores"), "coresused": c.get("coresused"),
        "memory_mb": c.get("memory"), "memoryused_mb": c.get("memoryused"),
        "workers": [dict(id=w["id"], host=w["host"], cores=w["cores"], coresused=w["coresused"],
                         memory=w["memory"], state=w["state"]) for w in c.get("workers", [])],
        "activeapps": [dict(id=a["id"], name=a["name"], cores=a.get("cores")) for a in c.get("activeapps", [])],
    }


@app.post("/api/run/{job_id}")
def run_job(job_id: str, body: dict | None = None):
    if job_id not in JOBS:
        raise HTTPException(404, "unknown job")
    body = body or {}
    total = int(os.environ.get("WORKER_CORES", "8")) * 2
    cores = max(1, min(int(body.get("cores") or total), total))
    allowed = {p["name"] for p in JOBS[job_id].get("params", [])}
    params = {k: v for k, v in (body.get("params") or {}).items() if k in allowed}
    for p in JOBS[job_id].get("params", []):
        if "choices" in p and p["name"] in params and params[p["name"]] not in p["choices"]:
            raise HTTPException(400, f"{p['name']} must be one of {p['choices']}")
    run = dict(id=time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4], job=job_id,
               title=JOBS[job_id]["title"], params=params, cores=cores, status="queued",
               queued=time.time())
    with _lock:
        RUNS[run["id"]] = run
    _queue.put(run)
    return run


@app.get("/api/runs")
def runs():
    return sorted(RUNS.values(), key=lambda r: r["queued"], reverse=True)[:50]


@app.get("/api/runs/{run_id}")
def run_status(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(404)
    out = dict(run)
    log = LOGS / f"{run_id}.log"
    if log.exists():
        lines = log.read_text(errors="replace").splitlines()
        keep = [l for l in lines if l.startswith(("[step]", "[note]", "[result]", "Traceback", "  File"))
                or "Error" in l or "Exception" in l]
        out["log"] = keep[-40:]
        if run["status"] == "failed":
            out["log_tail"] = lines[-60:]
    if run.get("started") and run["status"] == "running":
        out["elapsed"] = round(time.time() - run["started"], 1)
    res = RESULTS / f"{run_id}.json"
    if res.exists():
        out["result"] = json.loads(res.read_text())
    return out


@app.post("/api/runs/{run_id}/cancel")
def run_cancel(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        raise HTTPException(404)
    if run["status"] in ("queued", "running"):
        run["status"] = "cancelled"
        p = PROCS.get(run_id)
        if p and p.poll() is None:
            p.terminate()
    return run


@app.get("/api/bigdata")
def bigdata():
    sets = {}
    for d in BIG.glob("*_x*"):
        kind, _, n = d.name.partition("_x")
        if kind not in ("weblogs", "parquet") or not n.isdigit():
            continue
        size = sum(f.stat().st_size for f in d.glob("*.csv" if kind == "weblogs" else "*.parquet"))
        sets.setdefault(int(n), dict(factor=int(n), gb=None, parquet_gb=None))[
            "gb" if kind == "weblogs" else "parquet_gb"] = round(size / 1024 ** 3, 2)
    out = [sets[k] for k in sorted(sets)]
    st = os.statvfs("/work")
    return {"datasets": out, "disk_free_gb": round(st.f_bavail * st.f_frsize / 1024 ** 3)}


@app.delete("/api/bigdata/{factor}")
def bigdata_delete(factor: int, fmt: str = "csv"):
    if fmt not in ("csv", "parquet"):
        raise HTTPException(400)
    d = BIG / f"{'weblogs' if fmt == 'csv' else 'parquet'}_x{factor}"
    if not d.is_dir():
        raise HTTPException(404)
    shutil.rmtree(d)
    return {"deleted": factor, "fmt": fmt}


@app.get("/api/source/{job_id}", response_class=PlainTextResponse)
def source(job_id: str):
    if job_id in STREAM_DEFS:
        return "\n\n".join(f"# ===== {f} =====\n" + (JOBS_DIR / f).read_text()
                           for f in (STREAM_DEFS[job_id]["file"], "stream_producers.py"))
    if job_id not in JOBS:
        raise HTTPException(404)
    return (JOBS_DIR / JOBS[job_id]["file"]).read_text()


# ---------------- streaming demos (long-running: producer + Spark streaming app) -------------
STREAM_DIR = Path("/work/stream")
STREAM_DIR.mkdir(parents=True, exist_ok=True)
STREAM_DEFS = {
    "ec_logs": dict(file="stream_ec_logs.py", title="電商即時統計"),
    "tweets": dict(file="stream_tweets.py", title="推文即時情緒"),
}
STREAMS = {}


def _alive(p):
    return p is not None and p.poll() is None


@app.post("/api/stream/{name}/start")
def stream_start(name: str, body: dict | None = None):
    if name not in STREAM_DEFS:
        raise HTTPException(404)
    stream_stop(name)
    rate = max(1, min(int((body or {}).get("rate", 20)), 5000))
    for f in STREAM_DIR.glob(f"{name}_*.json"):
        f.unlink()
    log = open(LOGS / f"stream_{name}.log", "w")
    producer = subprocess.Popen(["python", str(JOBS_DIR / "stream_producers.py"), name, "--rate", str(rate)],
                                stdout=log, stderr=subprocess.STDOUT, cwd="/opt/lab")
    time.sleep(3)                                   # let the producer create the topic
    spark = subprocess.Popen(["spark-submit", "--name", STREAM_DEFS[name]["title"],
                              "--conf", "spark.driver.host=portal", "--conf", "spark.cores.max=2",
                              "--conf", "spark.sql.shuffle.partitions=4", "--conf", "spark.executor.memory=1g",
                              str(JOBS_DIR / STREAM_DEFS[name]["file"])],
                             stdout=log, stderr=subprocess.STDOUT, cwd="/opt/lab")
    STREAMS[name] = dict(producer=producer, spark=spark, rate=rate, started=time.time())
    return stream_status(name)


@app.post("/api/stream/{name}/stop")
def stream_stop(name: str):
    s = STREAMS.pop(name, None)
    if s:
        for p in (s["producer"], s["spark"]):
            if _alive(p):
                p.terminate()
                try:
                    p.wait(15)
                except subprocess.TimeoutExpired:
                    p.kill()
    for d in (WORK_DIR / "checkpoints").glob(f"{name}_*"):     # each start uses a fresh one
        shutil.rmtree(d, ignore_errors=True)
    return {"stopped": name}


@app.get("/api/stream/{name}")
def stream_status(name: str):
    s = STREAMS.get(name)
    out = {"name": name, "running": bool(s and _alive(s["spark"])),
           "producer_running": bool(s and _alive(s["producer"])),
           "rate": s["rate"] if s else None,
           "uptime": round(time.time() - s["started"]) if s else 0}
    for key, f in (("producer", f"{name}_producer.json"), ("snapshot", f"{name}_latest.json")):
        p = STREAM_DIR / f
        if p.exists():
            try:
                out[key] = json.loads(p.read_text())
            except json.JSONDecodeError:
                pass
    if s and not out["running"]:
        out["log_tail"] = (LOGS / f"stream_{name}.log").read_text(errors="replace").splitlines()[-30:]
    return out
