# E-Commerce Data Platform

An end-to-end data engineering project built to learn (and demonstrate) Kafka, Spark, Delta Lake,
Airflow, dbt, Snowflake, Terraform and CI/CD on the public **Olist Brazilian e-commerce** dataset.

**Current status: Phase 1 (environment) and Phase 2 (Kafka producer) are in this package.**

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

### Step 7: Trigger your first Airflow DAG

In Airflow, find `hello_ecommerce_platform`, click the play button > **Trigger DAG**, and watch
the task turn green. That proves Airflow can see your `dags/` folder.

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
src/producers/            Kafka producers (order replay)
src/consumers/            Kafka consumers (peek tool)
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
| Everything slow | Raise Docker Desktop memory to 8 GB+ |

## Roadmap

| Phase | What you build | What you learn | Status |
|---|---|---|---|
| 1 | Local stack in Docker | containers, networking, volumes | in this package |
| 2 | Kafka producer replaying orders | topics, partitions, keys, serialization | in this package |
| 3 | Spark Structured Streaming into Bronze Delta tables | schemas, checkpoints, medallion | next |
| 4 | Silver layer: clean, dedupe, join all 9 tables | PySpark transforms, data quality | |
| 5 | Gold marts + CDC-style incremental loads | modeling, incremental processing | |
| 6 | Snowflake + dbt models and tests | warehouse modeling, dbt | |
| 7 | Airflow DAGs orchestrating the pipeline | scheduling, retries, alerting | |
| 8 | Data contracts, schema versioning, catalog | governance | |
| 9 | AWS (S3, Glue, Athena) via Terraform + GitHub Actions CI/CD | IaC, CI/CD | |
| 10 | Azure pass (ADF, Databricks, Synapse) | parity with your resume | stretch |

Read `docs/tool-guide.md` early: it explains when to use each tool and when not to, which is what
interviews actually test.
