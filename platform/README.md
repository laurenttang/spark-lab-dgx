# Spark Lab：運維手冊

把 2020 年的 Spark 課程（Spark 2.4.5 / Hadoop 2.10 / Hive 1.2 / Kafka 0.10）重建在 DGX Spark 上。
全部服務都跑在 Docker 裡，從區網用瀏覽器操作，不需要進終端機。

## 1. 使用方式

| 項目 | 內容 |
|---|---|
| 網址 | `http://<主機 IP>:8100/` |
| 帳號 / 密碼 | 見 `platform/.env` 的 `LAB_USER` / `LAB_PASSWORD`（檔案權限 600） |
| Demo 控制台 | `/` |
| Spark Master UI | `/spark/`（可看 worker、執行中的 app、DAG、stage、task） |
| History Server | `/history/`（已結束的 job） |
| JupyterLab | `/jupyter/lab`（notebook 存在 `platform/notebooks/`） |

所有容器都設定 `restart: unless-stopped`，主機開機後會自動啟動。

```bash
cd <repo>/platform
docker compose up -d            # 啟動 / 套用設定變更
docker compose ps               # 狀態
docker compose logs -f portal   # 看控制台 log
docker compose down             # 停止並移除容器（資料保留在 <repo>/work）
```

### 更換密碼
```bash
cd <repo>/platform
NEW='新密碼'
HASH=$(docker run --rm caddy:2.10-alpine caddy hash-password --plaintext "$NEW")
sed -i "s|^LAB_PASSWORD=.*|LAB_PASSWORD=$NEW|; s|^LAB_PASSWORD_HASH=.*|LAB_PASSWORD_HASH='$HASH'|" .env
docker compose up -d caddy
```

### 右上角的 Spark UI / History / Jupyter 怎麼看、怎麼用

| 按鈕 | 看什麼 | 什麼時候用 |
|---|---|---|
| **Spark UI** | **正在執行**的叢集和 job | job 跑的時候即時觀察 |
| **History** | **已經結束**的 job | job 跑完後回頭看細節、比較 |
| **Jupyter** | 自己寫程式的筆記本 | 不想只按網頁按鈕，想自己動手改程式 |

#### Spark UI（`/spark/`）：看叢集現在在做什麼

**首頁就是叢集總覽：**
- **Workers：** 2 個 worker，各 8 核、10 GB，狀態 ALIVE，可以看到每個 worker 用掉幾核。
- **Running Applications：** 正在跑的 job。
- **Completed Applications：** 最近結束的 job，只有名稱和耗時。細節要到 History 看。

**最好的用法：邊跑邊看。** 在控制台按一個比較久的 job（例如「② Spark 分析大量資料」×30），馬上開 Spark UI，在 Running Applications 點那個 job 的名稱，就會進入它自己的頁面。

| 分頁 | 看什麼 |
|---|---|
| **Jobs** | 每次 `collect()`、`count()` 這類動作都算一個 job。看得到進度條和 **DAG**（資料怎麼流動） |
| **Stages** | 每個 job 被切成幾個 stage、每個 stage 有幾個 task。點進去可以看 task 分配在哪個 worker，以及有沒有某個 task 特別慢（資料傾斜） |
| **Storage** | `.cache()` 的資料佔了多少記憶體，例如 CSV 版本的分析會在這裡看到快取 |
| **Executors** | 每個 executor 的核心數、記憶體、跑了幾個 task、GC 花了多久 |
| **SQL / DataFrame** | **最有價值的分頁。** 每個查詢的執行計畫圖，看得到讀了幾筆、shuffle 了多少資料 |
| **Structured Streaming** | 只有 stream 在跑時才會出現。即時顯示每秒輸入筆數、處理時間 |

**注意：** job 一結束，它的頁面就會從 Spark UI 消失，之後要改到 History 看。

#### History（`/history/`）：回頭看已經結束的 job

列出所有已完成的 job，保留 **14 天**。點任何一個進去，**分頁和 Spark UI 完全一樣**，只是內容停在 job 結束那一刻。

**適合拿來做的事：**
- **比較擴展性測試：** 1 核和 16 核的同一個 job，在 Stages 頁看 task 是排隊一個一個跑，還是同時跑。
- **比較 CSV 和 Parquet：** 到 SQL 分頁點開查詢。Parquet 版的掃描節點會顯示只讀了 5 個欄位（`ReadSchema`），CSV 版則要解析整行。
- **確認 GPU 真的有在算：** 「⑤ GPU 加速」的 job 進入 SQL 分頁，GPU 那一輪的執行計畫全是 `GpuFileGpuScan`、`GpuHashAggregate` 這類 `Gpu` 開頭的節點；CPU 那一輪則是一般名稱。這是最直接的證據。
- **job 失敗時找原因：** 看是哪個 stage 失敗、錯誤訊息是什麼。

**小提醒：** job 剛結束時，History 可能要等 10～20 秒才會出現，重新整理一下就好。

#### Jupyter（`/jupyter/lab`）：自己寫 PySpark

網頁版的 Python 筆記本，可以一格一格執行程式、馬上看到結果。

- **從這裡開始：** 左邊檔案列表打開 **`00_start_here.ipynb`**。第一格已經寫好連線到叢集的設定，按 Shift+Enter 逐格執行，裡面有 wordcount、犯罪資料 SQL 查詢等範例。
- **資料在哪裡：** `/data/course/` 是原始課程資料（唯讀）；`/work/` 是所有輸出，例如 `/work/bigdata/parquet_x30`、`/work/ec_web_logs_analysis/tables/`。
- **你自己的 notebook** 會存在主機的 `platform/notebooks/`，容器重建也不會不見，也會跟著 git 版本控制。

**⚠️ 用完要釋放資源。** Jupyter 一連上叢集，就會**佔住 8 核**，直到你關閉為止。這段期間你在控制台按的 job 只能用剩下的 8 核，擴展性測試的數字就會不準。用完記得在 notebook 最後執行 `spark.stop()`，或在 Jupyter 選單選 **Kernel → Shut Down Kernel**。要確認有沒有釋放，可以到 Spark UI 首頁看 Running Applications 裡還有沒有 Jupyter 的 app。

#### 建議的體驗順序

1. 控制台按「② Spark 分析大量資料」（×30、16 核）。
2. 馬上開 **Spark UI**，點進正在跑的 job，看 Jobs 的進度條和 Stages 裡 task 分散到兩個 worker。
3. 跑完後開 **History**，找到同一個 job，到 SQL 分頁看執行計畫。
4. 想自己改查詢條件時，開 **Jupyter** 的 `00_start_here.ipynb` 動手寫，用完記得 `spark.stop()`。

## 2. 架構

```
瀏覽器（區網）─► :8100 caddy（basic auth）
                  ├─ /          portal（FastAPI，排隊執行 spark-submit，顯示結果；掛 GB10 GPU，只給 ⑤ GPU job 用）
                  ├─ /spark     spark-master:8080（reverse proxy 到 worker 與 app UI）
                  ├─ /history   spark-history:18080
                  └─ /jupyter   jupyter:8888
spark-master ◄── spark-worker-1 / spark-worker-2（各 8 cores / 10 GB）
kafka（KRaft 單節點，不需要 ZooKeeper）
```

### 為什麼是 8 個容器

每個容器只負責一件事（Docker 的常見做法），某一個出問題可以單獨重啟，不會拖垮其他的。

| # | 容器 | 角色 | 為什麼需要 |
|---|---|---|---|
| 1 | `spark-master` | Spark 叢集的管理者 | 接收 job、分配給 worker，對應當年課程的 `spark://devenv:7077` |
| 2 | `spark-worker-1` | 實際運算的機器（8 核 / 10 GB） | 真正跑計算的地方 |
| 3 | `spark-worker-2` | 第二台運算機器（8 核 / 10 GB） | 模擬「多台機器的叢集」，在 Spark UI 上看得到工作被分給不同 worker，這是 Spark 分散式運算的核心概念 |
| 4 | `spark-history` | History Server | job 結束後還能回頭看 DAG、stage、執行計畫（`/history`） |
| 5 | `kafka` | 訊息佇列 | streaming 範例需要它：producer 送資料進去，Spark 從裡面即時讀取 |
| 6 | `portal` | 網頁控制台（FastAPI） | 網頁上按「執行」，就是它去下 `spark-submit`；也是唯一掛 GPU 的容器 |
| 7 | `jupyter` | JupyterLab | 想自己寫 PySpark 時用（`/jupyter`） |
| 8 | `caddy` | 唯一的對外入口（連接埠 8100） | 負責帳號密碼登入，再把 `/spark`、`/history`、`/jupyter`、`/` 轉給後面對應的容器 |

- **1～3 是 Spark 叢集本體。** 當年課程要好幾台機器，這裡用容器在一台主機上模擬。
- **4、5 是叢集的周邊服務。**
- **6、7 是給人操作的介面。**
- **8 是大門。** 只有它對區網開放，其他 7 個都躲在它後面，區網上的人無法繞過登入直接連進去。

**資源**：閒置時 8 個加起來約 1.6 GB 記憶體，CPU 幾乎是 0。worker 的 2 × 10 GB 是上限，只有 job 在跑時才會真的用到。

**可以少一點嗎**：不寫 notebook 可以停 `jupyter`，不看歷史紀錄可以停 `spark-history`，不跑 streaming 可以停 `kafka`
（例如 `docker compose stop jupyter`）。兩個 worker 也可以合併成一個（刪掉 `spark-worker-2`、把 `.env` 的 `WORKER_CORES` 改成 16），
但這樣在 Spark UI 上就看不到「工作分散到不同機器」的效果了。

| 路徑 | 用途 |
|---|---|
| `platform/image/Dockerfile` | 共用映像檔 `spark-lab:4.1.2` |
| `platform/docker-compose.yml`、`.env` | 服務定義、資源上限、登入設定 |
| `platform/conf/spark-defaults.conf` | 所有 Spark 程式共用的設定 |
| `platform/jobs/*.py` | 重寫後的課程程式（一般的 PySpark 程式，也能直接 `spark-submit`） |
| `platform/portal/` | 控制台（`catalog.py` 是範例清單） |
| `data/` | 課程資料集（一部分在 git 裡，其餘由 `tools/prepare_data.py` 下載或產生，見 `NOTICE.md`），以唯讀方式掛到 `/data/course` |
| `work/` | 所有輸出，掛到 `/work`：results、logs、spark-events、bigdata、ec_web_logs_analysis… |

## 3. 資源

- 閒置時所有容器加起來約 1.6 GB RAM，CPU 幾乎是 0。
- 尖峰時的上限：worker 2 × 12 GB、portal 24 GB（GPU job 用）、jupyter 8 GB、history 2 GB、kafka 1 GB，合計約 61 GB。
- GPU：只有「⑤ GPU 加速」執行時才會用到，約 8.7 GB 的 RMM pool，job 結束就釋放。其他時間 GPU 留給主機上的其他服務。
- 要調整就改 `.env` 的 `WORKER_CORES` / `WORKER_MEMORY` / `WORKER_MEM_LIMIT`，再執行 `docker compose up -d`。
- DGX Spark 的 CPU 和 GPU **共用 128 GB 記憶體**。主機上若還有其他 GPU 服務，設上限能避免互相排擠。
- 磁碟：映像檔約 5 GB，課程資料約 1 GB，放大資料（×30 ≈ 7 GB、×200 ≈ 45 GB；Parquet 版本約為 CSV 的 1/4）可在控制台刪除。

### 長期運作（主機不重開機）

會持續累積的東西都有上限，不需要定期手動清理：

| 項目 | 上限 / 清理方式 | 設定位置 |
|---|---|---|
| Docker 容器 log | 每個容器 10 MB × 3 份 | `docker-compose.yml` 的 `x-logging` |
| Spark event log（History） | 14 天後自動刪除 | `spark-defaults.conf` 的 `spark.history.fs.cleaner.*` |
| worker 的 app 目錄（executor log、jar） | app 結束 3 天後刪除，每小時檢查一次 | `SPARK_WORKER_OPTS` 的 `spark.worker.cleanup.*` |
| 長時間執行的 stream 的 executor log | 每份 10 MB，保留 3 份 | `spark.executor.logs.rolling.*` |
| Kafka 訊息 | 24 小時或每個 partition 1 GB（segment 每小時切換一次，保留期限才會真的生效） | `KAFKA_LOG_*` |
| streaming checkpoint | stream 啟動、停止時刪除（每次都從 latest 開始，舊的沒有用） | `portal/app.py` 的 `stream_stop` |
| 執行紀錄（results / logs） | 只保留最新 300 次（約 10 MB） | 環境變數 `KEEP_RUNS` |
| Watermark 實驗的 Kafka topic、checkpoint | 實驗結束就刪除（每次用新的 `watermark_demo_<時間>`） | `jobs/adv_watermark.py` |
| Delta 表（`work/delta/`） | 每次執行先刪掉同倍數的舊表再重建，每個倍數只留一份（×30 約 1.8 GB） | `jobs/adv_delta.py` |
| Delta jar 快取（`work/ivy/`） | 固定約 175 MB，下載一次之後重複使用 | `portal/app.py` 的 `--packages` |
| producer log | 每分鐘印一行 | `jobs/stream_producers.py` |

**唯一會一直變大的是放大資料**（`<repo>/work/bigdata/`，×30 約 6.7 GB CSV + 1.8 GB Parquet），只有在控制台按「資料放大」時才會增加，不用時在控制台刪除即可。
檢查用量：`du -sh <repo>/work/*`。

## 4. 驗證指令

```bash
cd <repo>/platform && set -a && . ./.env && set +a
# 1) 沒登入會被擋（預期 401）
curl -s -o /dev/null -w '%{http_code}\n' localhost:8100/
# 2) 叢集狀態：預期 2 個 ALIVE worker、16 cores
curl -s -u "$LAB_USER:$LAB_PASSWORD" localhost:8100/api/cluster
# 3) 映像檔確實是 aarch64 原生版本
docker run --rm --entrypoint uname spark-lab:4.1.2 -m      # aarch64
# 4) Kafka 連接器已打包進映像檔
docker run --rm --entrypoint bash spark-lab:4.1.2 -c 'ls $SPARK_HOME/jars | grep kafka'
```

### GPU 驗證（「⑤ GPU 加速」）

```bash
cd <repo>/platform && set -a && . ./.env && set +a
# 5) portal 容器看得到 GB10
docker compose exec portal nvidia-smi -L                 # GPU 0: NVIDIA GB10
# 6) RAPIDS jar 有掛進來，而且是 cuda13-arm64 版本
docker compose exec portal ls /opt/rapids                # rapids-4-spark_2.13-26.10.0-cuda13-arm64.jar
# 7) 從控制台或 API 執行 GPU job，看結果裡的「GPU 運算子」「GPU 使用率峰值」「結果一致 ✓」
curl -s -u "$LAB_USER:$LAB_PASSWORD" -H 'Content-Type: application/json' \
  -d '{"params":{"factor":30,"formats":"parquet"},"cores":16}' localhost:8100/api/run/gpu
# 8) 沒有退回 CPU：log 裡不應該出現「!Exec ... cannot run on GPU」
grep -c '^ *!Exec' <repo>/work/logs/<run id>.log        # 預期 0
# 9) 執行中另開終端機看 GPU 有沒有在動
nvidia-smi --query-gpu=utilization.gpu --format=csv -l 1
```

History Server（`/history/`）裡的 GPU app，在 SQL 分頁的執行計畫會看到 `GpuFileGpuScan`、`GpuHashAggregate` 等 `Gpu*` 運算子。
spark-worker 容器**沒有**掛 GPU，一般的 cluster job 還是純 CPU。

## 5. 為什麼選這個方案

| 決定 | 原因 |
|---|---|
| Spark **4.1.2**（不是 2.4.5） | 2.4.5 不支援 Python 3.8 以上，也沒有 aarch64 的官方映像檔。4.1.2 是 2026/07 發布的穩定版，比 4.2 / 4.3 成熟。 |
| 基底映像檔 `quay.io/jupyter/pyspark-notebook:spark-4.1.2` | 官方多架構映像檔，有原生 aarch64 版本，內含 Java 21、Python 3.13、JupyterLab，一個映像檔就能涵蓋 master、worker、jupyter、portal。 |
| Spark Standalone，2 個 worker | 模擬當年 `spark://devenv:7077` 的叢集概念，在 Spark UI 上能看到工作被分配到不同 worker。 |
| 不裝 HDFS / YARN / Hive Metastore | 單機用不到。Spark 內建 catalog 可以直接執行原本的 HiveQL，`hdfs://devenv/...` 都改成本機路徑。 |
| DStream 改寫成 Structured Streaming | `spark-streaming-kafka-0-8` 在 Spark 3.0 就被移除了。`window(60, 5)`、`window(300, 20)` 改成 `F.window(...)`。 |
| Kafka 4.3.2（KRaft） | 新版 Kafka 已經不需要 ZooKeeper；課程筆記裡的 `--zookeeper` 參數也已失效。 |
| 用 MySQL + Tableau 的部分改成 Parquet + 控制台圖表 | 少一個資料庫要維護；Tableau 需要授權。輸出表格存在 `/work/ec_web_logs_analysis/tables/`，名稱和原本的 MySQL 表相同。 |
| 擴展性測試加上 Parquet（`power_to_parquet.py`、`power_query.py --format parquet`） | 對應課程的 Big Data File Formats 單元：同一份資料、同一個查詢，只換檔案格式，直接比較耗時和檔案大小。 |
| GPU job 用 portal 容器的 local mode，不改 worker | 只有一顆 GPU，RAPIDS 建議一個 executor 對一顆 GPU。兩個 worker 共用同一顆 GPU 需要額外的 GPU 資源排程，收益不大。在同一個 session 裡切換 `spark.rapids.sql.enabled` 做 CPU / GPU 對照，最公平。 |
| Delta Lake 用 `--packages` 在執行時載入，不打包進映像檔 | 只有「Delta Lake」這個 job 需要。打包進 `$SPARK_HOME/jars` 會讓每個 job 都載入 Delta，還可能和 RAPIDS 的 Delta 支援互相干擾；`spark_catalog` 換成 DeltaCatalog 也只套用在這個 job。jar 存在 `/work/ivy`，第一次執行下載後就不用再抓。 |
| 選 Delta Lake 而不是 Iceberg | 兩者都有支援 Spark 4.1 的正式版本（Delta 4.4.1 `delta-spark_4.1_2.13`、Iceberg 1.12.0）。Delta 不需要另外設定 catalog，直接用路徑就能 MERGE / 時間旅行，最適合當入門示範。觀念（交易紀錄、snapshot、檔案層級統計）兩者相通。 |
| Watermark 做成一次性的實驗，不是常駐 stream | 自己產生事件，才知道「真實答案」，能算出完整度和結果延遲。同一個 topic 給多個查詢同時讀、只差 watermark，比較才公平。 |
| quadkey 改用 `mercantile` 計算 | 原本的 `lib_quadkey.zip` 遺失了；mercantile 用的是同一套 Bing Maps tile 規則，算出的 quadkey 一樣。 |

## 6. GPU（RAPIDS Accelerator for Apache Spark）評估（2026-11-14）

### 支援狀況（啟用前查證）

| 項目 | 官方說明 | 這台機器 | 結論 |
|---|---|---|---|
| 版本 | RAPIDS Accelerator **26.10.0**（專案現名 cudf-spark） | — | — |
| Spark | Scala 2.13：3.5.x、4.0.0–4.0.4、**4.1.1–4.1.3**、4.2.0 | Spark 4.1.2 / Scala 2.13 | ✓，載入的 shim 是 `spark412` |
| CPU 架構 | x86_64、**arm64** | aarch64 | ✓，classifier `cuda13-arm64` |
| CUDA / 驅動 | 以 CUDA 12.9 / **13.1** 編譯；CUDA 13 需要驅動 **R580+** | 驅動 580.173.02 | ✓ |
| GPU | Volta 到 **Blackwell** 世代；測試過 B100、GB100、RTX PRO。**沒有列出 GB10 / sm_121** | GB10（compute capability 12.1） | 官方沒有列出，**實測可用**（見下） |

不需要自己編譯（所以沒有 `sm_121` 架構參數的問題）。jar 從 Maven Central 下載，SHA1 `2f23f65a1a6fa8ed75dcb90a02b306e3d28924f9` 和官方的 `.sha1` 相符。
放在 `platform/image/rapids/`（538 MB），只掛進 portal，沒有打包進映像檔。

### 實測（×30 = 3,830 萬筆，local[16]，4 個查詢合計）

| 格式 | CPU | GPU | 加速 | 結果一致 | GPU 使用率峰值 |
|---|---|---|---|---|---|
| Parquet（1.8 GB） | 10.2 s | 5.1 s | ×2.0 | ✓ | 73% |
| CSV（6.7 GB） | 60.4 s | 18.1 s | ×3.3 | ✓ | 96% |

- 這是暖機後的數字（資料已經在 OS 快取裡，JIT 也跑過），比的是運算本身。冷啟動的第一次執行兩邊都會慢很多。
- 所有運算子都在 GPU 上執行（`GpuFileGpuScan`、`GpuFilter`、`GpuProject`、`GpuHashAggregate`、`GpuColumnarExchange`、`GpuSort`、`GpuTopN`…），沒有退回 CPU。
- CSV 加速最多，因為 GPU 很擅長解析文字。Parquet 本來就很快，剩下的時間大多是固定成本。
- 這批資料不大，加速有限。GB10 的 GPU 和 CPU 共用 LPDDR5X 記憶體，頻寬遠低於 HBM 資料中心 GPU，所以不要期待 H100 那種幾十倍的加速。

### 為什麼沒讓整個 cluster 都用 GPU

1. 只有一顆 GPU，而且和 CPU 共用記憶體；開發時主機上還有其他 AI 服務長期佔用約 29 GB。
2. 大部分課程範例資料很小，啟動成本比運算本身還大，用 GPU 沒有意義。Python UDF（AFINN、quadkey、sklearn）也不會被 RAPIDS 加速。
3. 所以只把 GPU 做成一個獨立的對照 demo（「⑤ GPU 加速」），其他 job 維持純 CPU。

## 7. 踩過的雷

| 問題 | 解法 |
|---|---|
| `docker build` 時 `import pyspark` 失敗 | 基底映像檔只在 `start.sh` hook 裡設定 `PYTHONPATH`。Dockerfile 要自己設好 `PYTHONPATH`（包含 `$SPARK_HOME/python` 和 py4j zip）。 |
| 把 Kafka jar 複製到 `$SPARK_HOME/jars` 時 Permission denied | 下載 jar 時用 `spark.jars.ivy=/tmp/ivy`，複製這一步改用 `USER root`。 |
| Worker 一直重啟：`AccessDeniedException /usr/local/spark/work` | 設定 `SPARK_WORKER_DIR=/tmp/spark-work`。 |
| 除了 jupyter 以外的容器都顯示 unhealthy | 基底映像檔的 healthcheck 只適用於 Jupyter，其他服務設 `healthcheck: {disable: true}`。 |
| 每個 job 大約有 20–25 秒啟動成本 | 包含 driver JVM 啟動和 executor 註冊。小資料的範例主要都花在這裡，不是運算本身。`minRegisteredResourcesRatio=1.0` 讓步驟計時不包含這段等待。 |
| 原課程用的 MaxMind GeoLite2 不能再散布 | 公開版改用 DB-IP City Lite（CC BY 4.0，MMDB 格式相同，`geoip2` 程式不用改），由 `tools/prepare_data.py` 下載當月版本。 |
| Parquet 版本一開始只比 CSV 快一點（16 核 44 s 對 53 s） | 沿用了 CSV 的「先 `.cache()` 再查詢」。建 cache 本身就要 22 s（3,800 萬筆字串欄），比直接讀 Parquet 還貴。改成 CSV cache、Parquet 不 cache，各用最適合的策略（16 核 4 個查詢：cache 37 s 對直接讀 12.5 s）。 |
| `/api/bigdata` 把 `parquet_x30` 當成 CSV 倍數解析 | 目錄名稱改成依前綴（`weblogs_x` / `parquet_x`）分類，刪除 API 加上 `?fmt=csv|parquet`。 |
| RAPIDS 啟動失敗：`pool allocation ... was less than allocation of ... minAllocFraction: 0.25` | GB10 沒有獨立顯存，CUDA 回報的 total 約 75 GB，free 只有 17 GB 左右（其餘被系統和其他服務用掉）。預設要求 pool 至少是 total 的 25%。改成 `allocFraction=0.5`、`minAllocFraction=0.05`、`maxAllocFraction=0.5`，pool 約 8.7 GB。 |
| CSV 在 GPU 上只有一部分有加速 | 有兩個原因：(1) RAPIDS 不支援在 `Asia/Taipei` 時區解析 CSV 時間，scan 會退回 CPU（log：`Not supported timezone type Asia/Taipei`）；(2) 預設的 cache 格式不能上 GPU（`ParquetCachedBatchSerializer is not being used`）。解法：session 改用 UTC、小時 +8 換回台北時間（台灣沒有日光節約時間，換算是精確的），並設定 `spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer`。 |
| 以為 UTC 解析不影響小時（錯） | CSV 的時間帶有時區（原始資料是 `+08:00`，放大後的是 `Z`），改用 UTC 解析後 `hour()` 會差 8 小時。用 `(hour + 8) % 24` 修正後，已和 CPU 版本逐筆比對一致。 |
| RMM log 出現 `maximum pool size exceeded ... 2.000000 GiB` | 是 pinned host memory pool 不夠，RAPIDS 會改用一般記憶體，結果仍然正確。調成 `pinnedPool.size=4g` 後就沒有再出現。 |
| `bootstrap.sh` 在產生 `.env` 時沒有任何訊息就結束（2026-11-14 從全新 clone 測出） | `tr -dc … </dev/urandom \| head -c 14`：`head` 讀夠就關閉管線，`tr` 收到 SIGPIPE（exit 141），在 `set -o pipefail` 下整支腳本被中止。改成先 `head -c 64 /dev/urandom` 讀固定長度再過濾。另外加了 `trap … ERR`，失敗時會印出是哪一行。 |
| 全新環境 History Server 無限重啟：`Log directory specified does not exist: file:/work/spark-events` | `work/` 不在 git 裡，全新 clone 沒有這個目錄，所有 job 的 event log 也會失敗。`bootstrap.sh` 現在會先建立 `work/` 底下所有需要的目錄。 |
| 全新 clone 時資料準備失敗：Sentiment140 下載到一半 `TimeoutError` | 資料集伺服器很慢（約 150 KB/s，81 MB 要 9 分鐘），而且會中途停住。原本用 `urlopen().read()` 一次讀完，一停就整個失敗。改成分段寫檔、停住就用 HTTP Range 從中斷處續傳（最多 6 次），下載過的檔案快取在 `work/downloads/`，重跑不會再下載。 |
| 想在同一台主機測試 bootstrap，會和正在運作的服務衝突 | 用 `COMPOSE_PROJECT_NAME=<其他名稱>` 另起一組容器，`.env` 的 `LAB_PORT` 換成其他連接埠。測試會重新 build 並覆蓋 `spark-lab:4.1.2` 這個名稱，結束後用 `docker tag <原本的 ID> spark-lab:4.1.2` 改回原本的映像檔。 |
| `docker ps --size` 顯示 Kafka 容器有 1.26 GB | 是 Kafka 預先配置的索引檔（sparse file）造成的表面大小，實際資料只有幾 MB。 |
| `nvidia-smi` 的 memory.total / used 顯示 `[N/A]` | GB10 是統一記憶體架構，這是正常現象。GPU 使用量要看 `--query-compute-apps` 或 `utilization.gpu`。 |
| Delta：`OPTIMIZE ... ZORDER BY (timestamp)` 出現 `PARSE_SYNTAX_ERROR at or near 'OPTIMIZE'` | 錯誤訊息很誤導：真正原因是 `timestamp` 是 SQL 關鍵字，Delta 的語法解析失敗後交給 Spark 解析，才報 OPTIMIZE 錯。欄位名加反引號 `` `timestamp` `` 就好。 |
| Delta：OPTIMIZE ZORDER 之後 data skipping 沒效果 | OPTIMIZE 預設把檔案合併成約 1 GB，1.8 GB 的表只剩 2 個檔，跳不掉多少。示範時把 `spark.databricks.delta.optimize.maxFileSize` 設成 32 MB（約 56 個檔）。實際環境的檔案大小要依查詢模式決定。 |
| Watermark：Spark 回報的「丟棄列數」遠小於實際少算的事件 | `numRowsDroppedByWatermark` 算的是 state 運算子前、已經部分聚合過的列，不是原始事件數。要知道丟了多少事件，得像這個實驗一樣和真實筆數比對。 |
| Watermark：一開始的遲到事件都被收進來 | 第一個 micro-batch 還沒有 watermark（沒有下限），實驗剛開始那幾秒的遲到事件全部會被計入。比較時只算實驗開始後的完整視窗。 |
| Watermark：最後幾個視窗一直沒有輸出 | append 模式要等 watermark 超過視窗結束時間才輸出，而 watermark 只會被新事件推進。資料停了，最後的視窗就卡住。實驗最後送一筆未來 1 小時的事件把 watermark 推過去（它自己的視窗不列入比較）。 |
| Delta：VACUUM 之後 `versionAsOf 0` 的 `count()` 還是成功，以為舊版本還在 | Delta 用 log 裡每個檔的 `numRecords` 直接回答 `count()`，不開資料檔。真的讀資料（例如 `sum(length(device_id))`）才會出現 `FAILED_READ_FILE.FILE_NOT_EXIST`。驗證資料還在不在，不能只用 count。 |
| Python UDF 裡的模組 | executor 和 driver 用同一個映像檔，`/opt/lab/jobs` 在 PYTHONPATH 裡；`project_etl` 另外用 `addPyFile` 示範原本的 `--py-files` 用法。 |

## 8. 實測數據（2026-11-14，×30 = 3,830 萬筆）

| 核心 | CSV（6.7 GB） | Parquet（1.8 GB，實際讀 1.0 GB） | Parquet 快幾倍 |
|---|---|---|---|
| 1 | 239 s | 96 s | ×2.5 |
| 2 | 141 s | 61 s | ×2.3 |
| 4 | 88 s | 38 s | ×2.3 |
| 8 | 65 s | 34 s | ×1.9 |
| 16 | 56 s | 35 s | ×1.6 |
| pandas（單核） | 推估 141 s、9 GB RAM | 推估 46 s、5 GB RAM | ×3.1 |

- CSV → Parquet（snappy）：6.69 GB → 1.78 GB（3.8 : 1），16 核轉檔 54 s。查詢只用 5/15 欄，所以實際只讀 0.99 GB。
- 加速不是線性的：GB10 是 10 個 X925 + 10 個 A725 大小核，主機上的其他服務也長期佔用約 3–4 核。
- Parquet 在 8 核以後就不再變快。剩下約 30 s 多半是固定成本（4 個查詢各自排程、executor 的 JIT 暖機），不會隨核心數縮短。
  用暖機後的 executor 直接測，4 個查詢只要 12.5 s。要看到 Parquet 的擴展曲線，需要更大的資料（×100 以上）。
- 數據會受同時在跑的東西影響：同一時間在 portal 容器外另跑 spark-submit，CSV 1 核從 239 s 變成 315 s。正式測試時不要同時執行其他 job。

## 9. 資料來源

所有資料集的來源與授權見根目錄的 `NOTICE.md`。重點：

- 原始課程資料**沒有**放進這個 repo。可以再散布的（公有領域、OGL、Apache 2.0）直接放在 `data/`；
  授權不允許再散布的（MovieLens、Sentiment140、SMS Spam、Titanic、DB-IP）由 `tools/prepare_data.py` 下載。
- 電商 web logs、access log、Hive page views、股價是**模擬資料**。電商 log 依原課程資料的彙總統計分布產生
  （每小時流量、各年齡層的商品類別與裝置 / 連線方式、城市占比），沒有使用任何原始資料列，所以模型準確率、
  儀表板數字會和原課程不同。
- 產生器有固定亂數種子，每次 clone 產生的資料都一樣。重新產生：刪掉對應檔案後再執行
  `docker run --rm -v $PWD/../data:/data/course -v $PWD/../work:/work -v $PWD/../tools:/tools:ro --entrypoint python spark-lab:4.1.2 /tools/prepare_data.py`。

### 開發時的資料修復（原始課程資料，不在這個 repo 裡）

原始資料有好幾個檔案從 4 MB 或 8 MB 的位置開始被 0x00 覆蓋（推測是當年複製中斷）：從同目錄完好的 zip 還原、
從損毀的 zip 直接解 zlib 串流救回 1,397,618 / 1,600,000 筆推文、截掉 0x00 尾段保留完整的行。
課程程式原本不在備份裡，是依上課紀錄重建並移植到 Spark 4。

## 10. 進階主題（2026-11-16）

課程之後最值得深究的兩個方向，各做成一個可以直接執行的實驗，在控制台左邊的「進階主題」。

### Watermark 與遲到資料（`jobs/adv_watermark.py`）

1. job 自己當 producer，對一個新的 Kafka topic（3 個 partition）送 `duration` 秒的事件。每筆事件帶自己的事件時間；
   `late_pct`% 的事件故意遲到，分成 3–8 s、10–25 s、30–50 s 三組。
2. 同一個 topic 同時給多個串流查詢讀，**只差在 watermark**（預設 0 / 10 / 60 秒），都用 10 秒的事件時間視窗計數，append 模式輸出。
3. producer 知道自己送了什麼，所以能和每個查詢的輸出逐視窗比對。

實測（60 秒 × 200 筆/秒，遲到 30%）：

| watermark | 完整度 | 平均結果延遲 | state 最多列數 | 3–8 s 遲到 | 10–25 s 遲到 | 30–50 s 遲到 |
|---|---|---|---|---|---|---|
| 0 s | 88.5% | 3.5 s | 8 | 87% | 6% | 0% |
| 10 s | 94.4% | 10.7 s | 8 | 100% | 67% | 0% |
| 60 s | 100% | 23.4 s | 26 | 100% | 100% | 100% |

取捨很清楚：watermark 越長收得越完整，但每個視窗要多等這麼久才有結果，state 也要同時保留更多視窗。
實務上要依「資料最晚會晚多久」和「結果能等多久」決定，例如報表可以等，告警不能等。

```bash
# 預設參數執行（約 1.5 分鐘）
curl -s -u "$LAB_USER:$LAB_PASSWORD" -H 'Content-Type: application/json' -d '{}' localhost:8100/api/run/watermark
# 執行中看 topic（結束後會被刪掉）
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep watermark
```

### Delta Lake（`jobs/adv_delta.py`）

| 步驟 | 示範什麼 |
|---|---|
| ① Parquet → Delta | Delta = Parquet 資料檔 + `_delta_log/*.json` 交易紀錄。每個資料檔在 log 裡有筆數和每欄 min / max |
| ② 建 devices 表 | 每台裝置一列，當作要被更新的維度表 |
| ③ MERGE | 一個 commit 同時更新 5% 的裝置、新增 500 台、刪除 100 台（「被遺忘權」） |
| ④ UPDATE 忘了 WHERE | 故意把所有 gender 改成 NULL |
| ⑤ 時間旅行 + RESTORE | 用 `versionAsOf` 讀每個版本，確認錯誤，再 `RESTORE TABLE ... TO VERSION AS OF 1` 救回 |
| ⑥ OPTIMIZE ZORDER | 依 timestamp 重排；比較查一天的資料要開幾個檔 |
| ⑦ VACUUM | 刪掉舊檔省空間；之後就不能時間旅行回那些版本（示範用 `RETAIN 0 HOURS`，預設保留 7 天） |

實測（×30 = 3,830 萬筆，16 核，約 5.5 分鐘；用的是原始課程資料，這個 repo 附的模擬資料裝置數不同，MERGE、UPDATE 的筆數會不一樣）：

- 寫成 Delta：18 個檔、1.78 GB，交易紀錄只有 26 KB。
- MERGE：更新 1,539、新增 500、刪除 100；devices 表的 16 個檔被改寫成 1 個（copy-on-write）。
- 查 2019-05-18 一天：寫入後 18 個檔（1.78 GB）都得開，因為每個檔都混了整個月的資料；OPTIMIZE ZORDER 後 56 個檔只要開 2 個（0.035 GB）。
- OPTIMIZE 後磁碟上暫時有新舊兩份（2.96 GB），VACUUM 後剩 1.18 GB，比原本的 1.78 GB 還小：依時間排序後，相近的值放在一起，壓縮效果更好。

驗證 Delta 版本和 jar：

```bash
ls <repo>/work/ivy/jars | grep delta          # io.delta_delta-spark_4.1_2.13-4.4.1.jar、io.delta_delta-storage-4.4.1.jar …
ls <repo>/work/delta/weblogs_x30/_delta_log    # …00.json（寫入）、…01.json（OPTIMIZE）、…02/03.json（VACUUM START / END）
```

版本相容：Delta 從 4.x 起依 Spark 版本分開發佈（`delta-spark_4.0_2.13`、`delta-spark_4.1_2.13`、`delta-spark_4.2_2.13`）。
升級 Spark 時，`portal/catalog.py` 的 `DELTA_PKG` 要換成對應的 artifact。Delta 是純 JVM，沒有 aarch64 / GPU 相容問題。

