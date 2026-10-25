# Tool guide: what each tool is, when to use it, when NOT to

Interviewers rarely ask "what is Kafka?". They ask "why Kafka here and not X?".
For each tool, practice saying the *use when* and *don't use when* columns out loud.

## Ingestion and orchestration

| Tool | What it is | Use when | Don't use when | Where it appears here |
|---|---|---|---|---|
| **Kafka** | Distributed log. Producers write events to topics, consumers read them independently | Many systems need the same events in near real time; you need replay | Simple daily file loads (overkill) | Order stream (Phase 2) |
| **Airflow** | Scheduler for batch workflows (DAGs) with dependencies, retries, alerts | "Run B after A succeeds, retry 3 times, alert if late" | Millisecond streaming; it orchestrates work, it doesn't do the work | Phase 7 |
| **Fivetran / managed ELT** | No-code connectors that copy SaaS and database data into your warehouse | Common sources (Salesforce, Postgres, ads platforms) where hand-written ingestion adds no value | Custom or high-volume event streams | Concept only |
| **Azure Data Factory** | Azure's pipeline/orchestration service | Azure-native shops | You already run Airflow | Stretch module |
| **CDC (change data capture)** | Capture inserts/updates/deletes from a source DB's log instead of re-reading whole tables | Keeping a lake/warehouse in sync with an operational DB cheaply | Source is already an event stream | Phase 5 pattern |

## Processing and storage

| Tool | What it is | Use when | Don't use when | Where it appears here |
|---|---|---|---|---|
| **Spark / PySpark** | Distributed data processing engine (batch + streaming) | Data too big for one machine, or you need streaming joins/aggregations | Small data: pandas or SQL is simpler and faster | Phases 4-5 |
| **Delta Lake** | Table format on top of files: ACID transactions, updates/deletes, time travel | A lake needs reliable updates and history | Pure append-only archives where plain Parquet is enough | Bronze/Silver/Gold |
| **Apache Iceberg** | Competing open table format with similar goals; strong multi-engine support | You need one table readable by many engines | You are all-in on Databricks (Delta is the native fit) | Concept: know the trade-off |
| **S3 / ADLS** | Cheap object storage; the "lake" | Raw and processed files at any scale | Low-latency random lookups | MinIO locally, S3 in cloud |
| **Snowflake** | Cloud data warehouse: SQL analytics, elastic compute | Analysts and BI need fast, governed SQL | Heavy custom Python/ML on raw files (use Spark) | Phase 6 |
| **Hadoop / Hive / MapReduce** | The older big-data stack | Reading legacy systems | Starting anything new | Concept only |

## Transformation, quality, governance

| Tool | What it is | Use when | Don't use when | Where it appears here |
|---|---|---|---|---|
| **dbt** | SQL transformations as version-controlled, tested, documented code | Building Silver/Gold models in a warehouse | Row-level streaming logic | Phase 6 |
| **Data modeling** | Designing tables (facts/dimensions, star schema) so analysts query consistently | Always, before writing pipelines | Never skip it | Gold layer |
| **Data quality checks** | Automated tests: completeness, uniqueness, freshness, valid values | Before data reaches dashboards | Not optional | dbt tests + Spark checks |
| **Data contracts / schema versioning** | Agreement on a dataset's shape so producers can't silently break consumers | Multiple teams share data | Throwaway exploration | Kafka event schema (Phase 8) |
| **Unity Catalog / data catalog** | Central place for permissions, lineage, discovery | Many tables, many teams, RBAC needs | Single-person prototypes | Concept + lightweight catalog |
| **Data mesh** | Organizational pattern: each domain owns its data as a product | Large orgs where a central team is a bottleneck | Small teams (adds overhead) | Interview talking point |

## DevOps

| Tool | What it is | Use when | Don't use when | Where it appears here |
|---|---|---|---|---|
| **Docker** | Package an app with its dependencies into a container | Reproducible environments | Trivial scripts | Phase 1 (now) |
| **Terraform** | Define cloud infrastructure as code | Any cloud resource beyond a one-off experiment | Clicking around is genuinely fine for a 5-minute test | Phase 3 |
| **GitHub Actions / CI/CD** | Run tests and deploy automatically on every change | Team code, shared pipelines | Solo scratch work | Phase 9 |
| **Kubernetes** | Orchestrates many containers across machines | Large-scale production services | Local learning projects | Out of scope |

## The medallion pattern in one paragraph

**Bronze** is raw data exactly as it arrived (so you can always reprocess). **Silver** is
cleaned, deduplicated, typed and joined into business entities (orders, customers).
**Gold** is aggregated, analyst-ready tables (sales by month, delivery performance).
Each layer is rebuildable from the one before it, which is what makes failures recoverable.
