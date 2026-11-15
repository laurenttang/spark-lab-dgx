#!/usr/bin/env bash
# One-shot setup after `git clone`: create .env, fetch the RAPIDS jar, build the image,
# download / generate the datasets (inside the container), and start the stack.
#   ./bootstrap.sh            # everything
#   NO_GPU=1 ./bootstrap.sh   # skip the 538 MB RAPIDS jar (the ⑤ GPU job will not work)
# Host needs only Docker (Compose v2) and curl. Re-running is safe: finished steps are skipped.
set -euo pipefail
trap 'echo "bootstrap.sh failed at line $LINENO: $BASH_COMMAND" >&2' ERR
cd "$(dirname "$0")"
ROOT=$(cd .. && pwd)

echo "[1/6] work/ directories"
# work/ is not in git: create every dir the stack expects (History Server and every
# job's event log fail on start if /work/spark-events is missing)
mkdir -p "$ROOT"/work/{spark-events,spark-local,results,logs,stream,checkpoints,bigdata,warehouse,geo_src}

echo "[2/6] .env (login + resources)"
if [ ! -f .env ]; then
  # read a fixed amount first: `tr </dev/urandom | head` dies of SIGPIPE under pipefail
  PW=$(head -c 64 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | cut -c1-14)
  HASH=$(docker run --rm caddy:2.10-alpine caddy hash-password --plaintext "$PW")
  sed -e "s|^LAB_PASSWORD=.*|LAB_PASSWORD=$PW|" -e "s|^LAB_PASSWORD_HASH=.*|LAB_PASSWORD_HASH='$HASH'|" .env.example > .env
  chmod 600 .env
  echo "  created .env  (user: spark  password: $PW)"
fi

echo "[3/6] RAPIDS Accelerator jar"
JAR=image/rapids/rapids-4-spark_2.13-26.10.0-cuda13-arm64.jar
if [ -z "${NO_GPU:-}" ] && [ ! -f "$JAR" ]; then
  mkdir -p image/rapids
  curl -fsSL -o "$JAR" "https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/$(basename "$JAR")"
  echo "2f23f65a1a6fa8ed75dcb90a02b306e3d28924f9  $JAR" | sha1sum -c -
fi

echo "[4/6] build image"
docker compose build

echo "[5/6] datasets (download public ones, generate synthetic ones; a few minutes the first time)"
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$ROOT/data:/data/course" -v "$ROOT/work:/work" -v "$ROOT/tools:/tools:ro" \
  --entrypoint python spark-lab:4.1.2 /tools/prepare_data.py

echo "[6/6] start"
docker compose up -d
echo "Done: http://$(hostname -I | awk '{print $1}'):$(grep ^LAB_PORT .env | cut -d= -f2)/"
