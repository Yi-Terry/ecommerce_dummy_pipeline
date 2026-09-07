# ecommerce_dummy_pipeline

A synthetic ecommerce clickstream generator and data pipeline. It simulates
realistic user browsing sessions (browse → view product → add to cart →
checkout → purchase), streams the resulting events to a sink of your choice,
and — via Airflow — lands them in Databricks as a bronze/silver Delta
pipeline.

Useful as a self-contained playground for practicing streaming ingestion,
orchestration, and medallion-architecture ELT without needing a real
production data source.

## Architecture

```
generator.py --sink {console|file|kafka|webhook|databricks}
                            │
        ┌───────────────────┼───────────────────────┐
        │                   │                        │
   console/file          Kafka topic            Databricks bronze
   (local debug)     (docker-compose.kafka)    (bronze.ecommerce_events)
        │                   │                        │
   verify_consumer.py       │                        │  merge_to_silver (silver.sql)
   (prints + stats)         │                        ▼
                             │              Databricks silver
                             │           (silver.ecommerce_events)
                             ▼
                     (any Kafka consumer)
```

In production use, Airflow (`airflow/dags/ecommerce_generator_Dag.py`) runs
on a 5-minute schedule: it triggers a short burst of the generator writing
straight to the Databricks bronze table, then runs `silver.sql` to
merge/dedupe new rows into the silver table.

## Project structure

```
.
├── data_generation/
│   ├── generator.py          # Event generator / session simulator (all sinks)
│   └── verify_consumer.py    # Kafka consumer for manual verification
├── airflow/
│   ├── dags/
│   │   └── ecommerce_generator_Dag.py   # Scheduled generator -> bronze -> silver DAG
│   ├── sql/
│   │   └── silver.sql        # Bronze -> silver MERGE, deduplicated by event_id
│   ├── docker-compose.airflow.yml   # Local Airflow stack (Postgres, scheduler, etc.)
│   ├── Dockerfile.airflow    # Airflow image + generator's Python deps
│   └── requirements-airflow.txt
├── docker-compose.kafka.yml  # Local single-broker Kafka for dev/testing
├── requirements.txt          # Python deps for running the generator locally
├── .env.example              # Template for required environment variables
└── transforms/                # (reserved for future transform code)
```

## Prerequisites

- Python 3.11+
- Docker + Docker Compose (for local Kafka and/or local Airflow)
- A Databricks SQL Warehouse (only needed for `--sink databricks` or the Airflow DAG)

## Setup

1. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy the environment template and fill in the values you need:

   ```bash
   cp .env.example .env
   ```

   | Variable | Used by | Purpose |
   |---|---|---|
   | `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | generator.py, verify_consumer.py | Auth for managed Kafka (e.g. Confluent Cloud) when using `SASL_SSL`/`SASL_PLAINTEXT` |
   | `DATABRICKS_SERVER_HOSTNAME` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN` | generator.py (`--sink databricks`) | Connect to a Databricks SQL Warehouse |
   | `AIRFLOW_CONN_DATABRICKS_ID` | Airflow | Databricks connection used by `merge_to_silver` |
   | `AIRFLOW_VAR_DATABRICKS_SERVER_HOSTNAME` / `AIRFLOW_VAR_DATABRICKS_HTTP_PATH` | Airflow | Injected as Airflow Variables, referenced in the DAG's `run_generator` task |
   | `AIRFLOW__API_AUTH__JWT_SECRET` | Airflow | Required secret for the Airflow API server auth manager |

   Never commit `.env` — it's already excluded via `.gitignore`.

## Usage

### Run the generator locally

```bash
# Print events to stdout
python data_generation/generator.py --sink console --sessions-per-min 30 --duration 60

# Write events to a newline-delimited JSON file
python data_generation/generator.py --sink file --output events.jsonl --duration 120

# Stream to Kafka
python data_generation/generator.py --sink kafka --kafka-topic ecommerce_events --duration 0   # 0 = run forever
```

Run `python data_generation/generator.py --help` for the full flag list
(catalog/user size, think-time bounds, random seed, log level, etc.).

### Local Kafka + verification

```bash
docker compose -f docker-compose.kafka.yml up -d
python data_generation/generator.py --sink kafka --duration 0 &
python data_generation/verify_consumer.py --topic ecommerce_events
```

`verify_consumer.py` prints each event as it arrives and, on exit
(Ctrl+C), a summary: event type counts and the purchase conversion rate
across sessions seen.

### Local Airflow (generator → Databricks bronze → silver)

```bash
docker compose -f airflow/docker-compose.airflow.yml up -d --build
```

Then, in the Airflow UI (http://localhost:8080, admin/admin):
1. Set Variables `databricks_server_hostname` and `databricks_http_path`.
2. Set Connection `databricks_id` (Databricks SQL Warehouse credentials).
3. Unpause the `ecommerce_generator_to_bronze` DAG.

The DAG runs every 5 minutes: `run_generator` bursts events into
`workspace.bronze.ecommerce_events` for 90 seconds, then `merge_to_silver`
runs `airflow/sql/silver.sql` to upsert new rows into
`workspace.silver.ecommerce_events`.

## Data model

Each event is a flat JSON object with fields that vary by `event_type`
(`page_view`, `product_view`, `add_to_cart`, `begin_checkout`, `purchase`),
always including `event_id`, `timestamp`, `session_id`, `user_id`, `device`,
and `referrer`. Bronze stores the raw JSON as-is; `silver.sql` parses it into
the typed `silver.ecommerce_events` table, deduplicated on `event_id`.

## Development notes

- `data_generation/generator.py` has no test suite yet; validate changes by
  running with `--sink console` and a fixed `--seed` for reproducible output.
- Airflow task logs land in `airflow/logs/` (gitignored) — check there first
  when debugging a failed DAG run.
- Secrets belong in `.env` / Airflow Variables & Connections, never in DAG
  code or CLI flags (the generator already prefers env vars over flags for
  Kafka SASL credentials for this reason).
