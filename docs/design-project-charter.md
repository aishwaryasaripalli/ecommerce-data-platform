# Design Doc: E-Commerce Data Platform

## Problem / Goal
Build an end-to-end data platform that simulates how a real e-commerce marketplace
ingests, processes, governs and serves order, customer, product and payment data, so
that every tool on my resume is exercised in one working system I can explain in interviews.

## Scope
- Real-time order event streaming (Kafka)
- Batch and streaming processing into a Delta Lake medallion architecture (PySpark)
- Cloud storage and warehousing (AWS S3, Snowflake)
- Transformation and testing (dbt)
- Orchestration (Airflow)
- Data quality, data contracts and governance
- Infrastructure as code and CI/CD (Terraform, GitHub Actions)
- Stretch: an equivalent Azure pipeline (ADF, Databricks, Synapse)

## Out of scope (for now)
- Real-time ML model serving
- Multi-cloud from day one (AWS first, Azure as a later module)
- Kubernetes (Docker Compose is enough at this scale)

## Data source
Olist Brazilian E-Commerce public dataset: 9 CSVs (orders, customers, order items,
payments, reviews, products, sellers, geolocation, category translations).
Orders are replayed in purchase-timestamp order to simulate a live feed.

## Architecture
```
Olist CSVs --(replay)--> Kafka topic "orders" --> Spark Structured Streaming
                                                        |
Other 8 tables --(batch load)---------------------------+
                                                        v
                                   Bronze (raw Delta)  ->  Silver (clean, conformed)
                                                        ->  Gold (business marts)
                                                        ->  Snowflake + dbt (tests, models)
Airflow orchestrates batch jobs. Terraform provisions cloud resources.
GitHub Actions runs tests on every pull request.
```

## Success criteria
- The Kafka producer streams orders end to end into Bronze
- Silver and Gold layers pass automated data-quality tests
- An Airflow DAG runs the full pipeline on a schedule with no manual steps
- All cloud infrastructure is reproducible with `terraform apply` from a clean account
