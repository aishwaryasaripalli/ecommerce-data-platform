# E-Commerce Data Platform

An end-to-end data engineering project built to learn (and demonstrate) Kafka, Spark, Delta Lake,
Airflow, dbt, Snowflake, Terraform and CI/CD on the public **Olist Brazilian e-commerce** dataset.

**Current status: Phases 1-5 done: local stack, Kafka order replay, Bronze/Silver/Gold Delta tables on MinIO with incremental CDC-style loads.**

---

## What you need installed

| Tool | Check it works |
|---|---|
| Docker Desktop (give it 4+ CPUs and 8+ GB RAM in Settings > Resources) | `docker --version` and `docker compose version` |
| VS Code (+ extensions: Docker, Python, Jupyter) | open it |
| Git | `git --version` |

---

## Quick start (about 15 minutes)

Run every command **one line at a time**, from the project folder. In VS Code: *Terminal > New Terminal*
opens already inside it.

### Step 1: Clear out the old environment (one time only)

Earlier versions of this project left containers behind that hold the same ports. Remove them:

```bash
bash scripts/reset_old_environment.sh
```

Type `y` when asked. If it says "no container named ...", that's fine.

### Step 2: Download the dataset

1. Open https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce (free account needed) and click **Download**.
2. Unzip it. Move all **9 CSV files** into `data/raw/olist/`.
3. Check:

```bash
bash scripts/check_data.sh
```

You should see 9 lines saying `OK`.

### Step 3: Start everything

```bash
docker compose up -d --build
```

The first run takes 5-10 minutes (downloads images, builds the workspace). Later starts take seconds.
Then:

```bash
docker compose ps
```

You should see these running: `kafka`, `kafka-ui`, `minio`, `postgres`, `airflow-webserver`,
`airflow-scheduler`, `workspace`. (`airflow-init` runs once and exits; that is correct.)

### Step 4: Open the tools

| Tool | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8085 | none |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin123 |
| Airflow | http://localhost:8080 | admin / admin |
| Jupyter | http://localhost:8888/?token=datalake123 | token in URL |

Airflow can take 1-2 minutes after startup. Refresh if it isn't ready.

### Step 5: Run the environment check notebook

Open `notebooks/00_check_environment.ipynb` in VS Code.
Click **Select Kernel > Existing Jupyter Server**, enter `http://localhost:8888`, and when asked for a
password/token enter `datalake123`. Run all four cells; each should succeed.

### Step 6: Run the producer

Test with 200 orders first:

```bash
docker compose exec workspace python /home/jovyan/src/producers/order_producer.py --limit 200
```

Then confirm they arrived, either of two ways:

```bash
docker compose exec workspace python /home/jovyan/src/consumers/peek_consumer.py --count 5
```

or open Kafka UI > Topics > `orders` > Messages.

Run the whole dataset (about 99,000 orders, takes several minutes; Ctrl+C stops it any time):

```bash
docker compose exec workspace python /home/jovyan/src/producers/order_producer.py
```

Tips: `--speedup 0` sends as fast as possible; `--dry-run` prints without touching Kafka.

The producer runs inside the `workspace` container, so `docker compose up -d --build` (or anything
that restarts `workspace`) stops it. To resume without re-sending orders, pass the last purchase
time that made it (the progress lines show it, or check Bronze's max `order_purchase_timestamp`):

```bash
docker compose exec workspace python /home/jovyan/src/producers/order_producer.py --start-after '2018-05-17 21:55:40'
```

### Step 7: Trigger your first Airflow DAG

In Airflow, find `hello_ecommerce_platform`, click the play button > **Trigger DAG**, and watch
the task turn green. That proves Airflow can see your `dags/` folder.

---

## Phase 3: Bronze layer (Kafka + CSVs -> Delta Lake on MinIO)

```
Kafka "orders" --(Spark Structured Streaming)--> s3a://lake/bronze/orders
8 other CSVs   --(Spark batch)------------------> s3a://lake/bronze/<table>
```

Bronze keeps data exactly as it arrived (all strings, duplicates included) plus metadata saying
where each row came from. Cleaning is Silver's job (Phase 4).

### Step 1: Rebuild the workspace (one time)

Phase 3 adds the Kafka, Delta and S3 connectors to the image. Wait until no producer is running
(the restart stops it), then:

```bash
docker compose up -d --build
```

### Step 2: Load the 8 reference tables (about 30 seconds)

```bash
docker compose exec workspace python /home/jovyan/src/batch/load_bronze_tables.py
```

Expect 8 lines ending in `Done.` (customers 99,441 rows, geolocation 1,000,163, ...). Safe to rerun:
each run overwrites the table and Delta keeps the old version.

### Step 3: Stream orders into Bronze

Process everything currently in the topic, then stop:

```bash
docker compose exec workspace python /home/jovyan/src/streaming/orders_to_bronze.py --available-now
```

Or leave it running so new orders land every 10 seconds (run the producer in a second terminal to
watch it happen; Ctrl+C stops it):

```bash
docker compose exec workspace python /home/jovyan/src/streaming/orders_to_bronze.py
```

Run it again any time: the checkpoint remembers which Kafka offsets were already written, so it
only picks up new orders. To rebuild `bronze/orders` from scratch, delete both
`lake/bronze/orders` and `lake/_checkpoints/bronze_orders` in the MinIO console first.

### Step 4: Look at the result

- Run `notebooks/01_bronze_check.ipynb`: row counts, a raw order, the duplicates, Delta history and time travel.
- MinIO console (http://localhost:9001) > bucket `lake` > `bronze/`: the Parquet files and `_delta_log/` folders.

---

## Phase 4: Silver layer (clean, typed, deduplicated, joined)

```
s3a://lake/bronze/<table> --(src/silver/transforms.py + quality checks)--> s3a://lake/silver/<table>
```

| Silver table | What changed from Bronze |
|---|---|
| `orders` | one row per order (the 600 duplicate test sends removed), timestamps typed, `delivery_days` and `is_delivered_late` added |
| `customers`, `sellers` | city names normalized (`São Paulo ` -> `sao paulo`) so they match geolocation |
| `order_items`, `order_payments` | ints and `decimal(12,2)` money (never float for money) |
| `order_reviews` | keyed on `(review_id, order_id)`: `review_id` alone isn't unique in Olist |
| `products` | misspelled source columns fixed (`lenght` -> `length`), English category joined in, 2 missing translations patched, no category -> `unknown` |
| `geolocation` | 1M raw points -> one point per zip prefix; points outside Brazil dropped |
| `order_items_enriched` | one row per order item with customer, product, seller, both locations, payment total and review score joined in |

### Step 1: Rebuild the workspace (one time)

Phase 4 adds `pytest` and mounts `tests/`. Make sure no producer or streaming job is running, then:

```bash
docker compose up -d --build
```

### Step 2: Run the unit tests (about 10 seconds)

```bash
docker compose exec workspace pytest /home/jovyan/tests -q
```

Expect `8 passed`. They run the transforms on tiny hand-made tables, with no Kafka or MinIO involved.

### Step 3: Build Silver

```bash
docker compose exec workspace python /home/jovyan/src/batch/build_silver.py
```

The job builds every table, runs 36 data quality checks, and **only writes to the lake if no
`error` check fails** (a pattern called write-audit-publish), so a bad run never replaces good data.
`warn` results are real quirks of the source data (for example 8 orders marked delivered with no
delivery date); they're reported and the build continues. Every run's results are saved to
`s3a://lake/_quality/silver_checks`.

Add `--dry-run` to build and check without writing anything.

### Step 4: Look at the result

Run `notebooks/02_silver_check.ipynb`: Bronze vs Silver counts, the typed schema, the quality
results, and two real questions answered from `order_items_enriched` (top categories by revenue;
whether late deliveries get worse reviews).

---

## Phase 5: Gold marts + incremental (CDC-style) loads

```
order_updates.py --> Kafka --> Bronze (append) --MERGE--> Silver orders --Change Data Feed--> Gold (only affected slices)
```

| Gold mart | One row per | Answers |
|---|---|---|
| `sales_daily` | purchase day x category | revenue, orders, items, freight (canceled/unavailable excluded) |
| `delivery_by_state` | customer state x month | delivery days, late rate, review score (delivered orders) |
| `seller_scorecard` | seller | volume, revenue, review score, late rate, first/last sale |

**Incremental, not rebuild.** Orders change after they're placed (shipped -> delivered, processing
-> canceled). `order_updates.py` publishes such changes to Kafka like a CDC tool would. Then:

- `silver_orders_incremental.py` reads only new Bronze rows (streaming checkpoint) and **MERGEs** them
  into `silver.orders`: newer events update, older ones are ignored, reruns change nothing.
- `build_gold.py` reads only the rows that changed in `silver.orders` (Delta **Change Data Feed**,
  tracked by a watermark), finds the days, state-months and sellers they touch, recomputes just those
  slices and MERGEs them into Gold. If a reference table changed, it rebuilds instead.
- `--verify` recomputes every mart from scratch and checks the incremental result matches it exactly.

### Step 1: One-time setup

Phase 5 adds a `last_event_at` column to Silver orders and switches on Change Data Feed, so rebuild
Silver once, then do the first Gold build:

```bash
docker compose exec workspace python /home/jovyan/src/batch/build_silver.py
```

```bash
docker compose exec workspace python /home/jovyan/src/batch/build_gold.py
```

### Step 2: Change some orders and watch them flow through

Send 50 deliveries and 20 cancellations to Kafka:

```bash
docker compose exec workspace python /home/jovyan/src/producers/order_updates.py --deliver 50 --cancel 20
```

Land them in Bronze, MERGE them into Silver, then update Gold incrementally and verify it:

```bash
docker compose exec workspace python /home/jovyan/src/streaming/orders_to_bronze.py --available-now
```

```bash
docker compose exec workspace python /home/jovyan/src/streaming/silver_orders_incremental.py --available-now
```

```bash
docker compose exec workspace python /home/jovyan/src/batch/build_gold.py --verify
```

Expect `70 inserted or updated` from Silver, `INCREMENTAL` lines from Gold, and `MATCH` for all
three marts. Run the last two again: nothing changes (the jobs are idempotent). Use `--seed 7` on
`order_updates.py` to change a different set of orders.

### Step 3: Look at the result

Run `notebooks/03_gold_check.ipynb`: monthly revenue, slowest states, worst-rated sellers, and one
changed order traced from Kafka to Silver's Change Data Feed.

---

## Day-to-day commands

```bash
docker compose up -d        # start everything
docker compose stop         # pause everything, keep data
docker compose down         # remove containers, keep data volumes
docker compose down -v      # remove containers AND all data (full reset)
docker compose logs -f kafka   # watch one service's logs (Ctrl+C to exit)
docker compose ps           # what's running?
```

## Project layout

```
docker-compose.yml        the whole local stack
docker/workspace/         Dockerfile for the Jupyter + PySpark container
dags/                     Airflow DAGs
src/common/               shared Spark session (Delta + MinIO settings)
src/producers/            Kafka producers (order replay, order update simulator)
src/consumers/            Kafka consumers (peek tool)
src/streaming/            Spark Structured Streaming jobs (Kafka -> Bronze, Bronze -> Silver MERGE)
src/batch/                Spark batch jobs (CSVs -> Bronze, Silver full build, Gold build)
src/silver/               Silver transforms + data quality checks
src/gold/                 Gold mart definitions
tests/                    pytest unit tests (run inside the workspace container)
notebooks/                Jupyter notebooks
data/raw/olist/           the 9 Olist CSVs (never committed to Git)
scripts/                  helper scripts
docs/                     design doc + tool guide (read tool-guide.md!)
```

## Troubleshooting (every one of these has happened)

| Symptom | Fix |
|---|---|
| `port is already allocated` | An old container still holds the port. Run `bash scripts/reset_old_environment.sh` |
| `zsh: no matches found: ...?token=...` | Put the URL in quotes: `curl "http://localhost:8888/..."` |
| `git: 'add.' is not a git command` / "unknown switch" | One command per line, and a space between `add` and `.` |
| `dquote>` prompt stuck | Press Ctrl+C |
| `FileNotFoundError ... olist_orders_dataset.csv` | Dataset not in `data/raw/olist/`. Run `bash scripts/check_data.sh` |
| `service "workspace" is not running` | Run `docker compose up -d` from the project folder |
| Notebook says "detecting kernels" forever | Enter `http://localhost:8888` (no token in URL) and give the token when prompted |
| Python code typed in the terminal gives `command not found: import` | That's Python; run it in a notebook cell, not the terminal |
| `No module named 'pyspark'` / `'common'`, or `Failed to find data source: kafka` / `delta` | Workspace is still the pre-Phase-3 image. Run `docker compose up -d --build` |
| Everything slow | Raise Docker Desktop memory to 8 GB+ |

## Roadmap

| Phase | What you build | What you learn | Status |
|---|---|---|---|
| 1 | Local stack in Docker | containers, networking, volumes | done |
| 2 | Kafka producer replaying orders | topics, partitions, keys, serialization | done |
| 3 | Spark Structured Streaming into Bronze Delta tables | schemas, checkpoints, medallion | done |
| 4 | Silver layer: clean, dedupe, join all 9 tables | PySpark transforms, data quality | done |
| 5 | Gold marts + CDC-style incremental loads | modeling, incremental processing | done |
| 6 | Snowflake + dbt models and tests | warehouse modeling, dbt | next |
| 7 | Airflow DAGs orchestrating the pipeline | scheduling, retries, alerting | |
| 8 | Data contracts, schema versioning, catalog | governance | |
| 9 | AWS (S3, Glue, Athena) via Terraform + GitHub Actions CI/CD | IaC, CI/CD | |
| 10 | Azure pass (ADF, Databricks, Synapse) | parity with your resume | stretch |

Read `docs/tool-guide.md` early: it explains when to use each tool and when not to, which is what
interviews actually test.
