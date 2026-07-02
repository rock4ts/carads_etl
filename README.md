# Car Intel ETL

Python ETL pipeline for collecting, normalizing, deduplicating, and archiving car classified ads from Russian marketplaces (Avito, Drom, Auto.ru, and others exposed by the parser API).

The pipeline ingests raw ad payloads from an external parser, stores them in MongoDB, indexes normalized documents in Elasticsearch, links duplicate listings across time, and eventually archives old raw data to object storage.

## Pipeline overview

```
Parser API  →  Ingestion  →  MongoDB (raw) + Elasticsearch (processed)
                                    ↓
                              Matching (dedup links)
                                    ↓
                              Archiving (S3 / Yandex Object Storage)
```

Each stage is idempotent and tracks progress in PostgreSQL (`upload_timestamps`, `marker_timestamps`, `archive_batches`).

| Storage | Role |
|---------|------|
| **MongoDB** | Raw ad payloads (`raw_ads` collection) |
| **Elasticsearch** | Normalized, searchable car ad documents |
| **PostgreSQL** | Pipeline cursors, archive batch metadata |
| **S3-compatible storage** | Long-term raw ad archives (gzip JSONL) |

## Services

### Ingestion (`app.services.ingestion_service`)

Fetches ad batches from the parser API per site, advancing a cursor based on each ad's `checked` timestamp.

For every batch it:

1. Saves raw payloads to MongoDB
2. Maps them to processed documents and upserts into Elasticsearch
3. Updates the site's `upload_timestamps` checkpoint in PostgreSQL

Sites without a row in `upload_timestamps` are skipped.

### Processing (`app.services.processing_service`)

The normalization logic lives in `app/services/processing_service/mapper.py` and runs inline during ingestion. The standalone processing service entrypoint is reserved for future batch processing and does not run a worker today.

### Matching (`app.services.matching_service`)

Scans new Elasticsearch documents (those without a `predecessor_id`) and links them to earlier duplicate listings on the same site. Matching uses configurable scoring based on price, mileage, and temporal windows.

Progress is tracked per site via `marker_timestamps` (processing cursor) and `upload_timestamps` (ingestion upper bound) in PostgreSQL. Linked documents get `predecessor_id` / `successor_id` fields.

Run incrementally after ingestion (standalone or via the pipeline runner):

```bash
uv run python -m app.services.matching_service.main
```

#### Matching service vs `scripts/backfill_matcher.py`

Both use the same duplicate-finding logic (`find_best_duplicate`), but they target different jobs:

| | **Matching service** | **`scripts/backfill_matcher.py`** |
|--|----------------------|-----------------------------------|
| **Purpose** | Ongoing production worker | One-time historical rebuild over existing index data |
| **When to use** | After each ingestion run / in cron | First-time setup or re-linking an index that already has ads but no (or broken) duplicate chains |
| **Progress state** | `marker_timestamps` + `upload_timestamps` | `backfill_matcher_states` (`next_from`, `reset_completed`) |
| **Site selection** | Sites with a row in `upload_timestamps` (optional `MATCHING_SITES` filter) | All sites found in Elasticsearch (or `MATCHING_SITES`) — no ingestion cursors required |
| **Documents processed** | Only ads without `predecessor_id` in the window `[marker, upload]` | All ads in monthly slices from a fixed start date (`2024-07-10`) through the current month |
| **Existing links** | Left unchanged; skips already-linked docs | Clears `predecessor_id`, `successor_id`, and `is_duplicate` once per site before processing |
| **Validation** | None | Full index scan at the end — checks bidirectional link consistency and detects cycles |
| **Resumable** | Yes — marker advances batch-by-batch | Yes — per-site monthly checkpoint in `backfill_matcher_states` |

Do **not** run the backfill script on a schedule alongside the matching service on the same index unless you intend to wipe and rebuild duplicate links. For day-to-day operation, use the matching service only.

```bash
uv run python scripts/backfill_matcher.py
```

### Archiving (`app.services.archiving_service`)

Moves raw MongoDB documents older than a retention threshold to S3-compatible object storage (Yandex Object Storage by default), then deletes them from MongoDB. Batches are partitioned by archive month and tracked in the `archive_batches` table. Only one archive worker can run at a time (PostgreSQL advisory lock).

### Pipeline runner (`app.services.pipeline_runner`)

Orchestrates the full pipeline in order:

1. Ingestion
2. Elasticsearch index refresh
3. Matching
4. Archiving

Use this for scheduled runs (e.g. cron). Individual services can also be run standalone.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) for dependency management
- Running instances of:
  - **MongoDB** — raw ad storage
  - **Elasticsearch** — processed ad index
  - **PostgreSQL** — pipeline state
  - **S3-compatible storage** — only required for the archiving service
- A **parser API key** (`PARSER_API_KEY`)

## Setup

```bash
# Install dependencies
uv sync

# Configure environment
cp .env.example .env
# Edit .env with your connection strings and API keys
```

### PostgreSQL state tables

State tables (`upload_timestamps`, `marker_timestamps`, `archive_batches`, `backfill_matcher_states`) are defined in `app/database/models.py`. Each Postgres-backed entrypoint calls `ensure_etl_state_tables()` from `app/database/session.py` before opening a session — this runs SQLAlchemy `create_all` and is idempotent (existing tables are left unchanged).

Called from:

- `run_ingestion()` — ingestion service
- `run_matcher()` — matching service
- `run_archive()` — archiving service
- `run_backfill()` — `scripts/backfill_matcher.py`

The pipeline runner inherits this through those stages. `build_postgres_session_factory()` only creates a session factory; it does not modify the schema.

### Seed site cursors

Before ingestion can run, each site needs an initial cursor in PostgreSQL:

```sql
INSERT INTO upload_timestamps (site, "timestamp")
VALUES
  ('avito', '2019-01-01 00:00:00'),
  ('drom',  '2019-01-01 00:00:00'),
  ('auto',  '2019-01-01 00:00:00');
```

Replace site names and starting timestamps as needed. The parser API accepts sites like `avito`, `drom`, and `auto`.

### Elasticsearch index

Create the processed index using the mapping in `index_example.json`, or let the first ingestion upsert create documents against an existing index configured via `PROCESSED_INDEX`.

## Running services

Run from the repository root:

```bash
# Full pipeline (ingestion → refresh → matching → archiving)
uv run python -m app.services.pipeline_runner.main

# Individual services
uv run python -m app.services.ingestion_service.main
uv run python -m app.services.matching_service.main
uv run python -m app.services.archiving_service.main
```

## Configuration

All services read from a `.env` file in the project root. Copy `.env.example` and adjust values for your environment — it lists every variable the services recognize, grouped by concern.

| Variable | Used by | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | All services | Logging verbosity (`DEBUG`, `INFO`, …) |
| `TELEGRAM_REPORTING_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID` | All services | Optional Telegram alerts |
| `TELEGRAM_PROGRESS_INTERVAL_MINUTES` | Ingestion, Matching | Minutes between long-run progress reports (default `30`) |
| `POSTGRES_DATABASE_URL` | All services | Pipeline state database |
| `DATABASE_URL` | Matching, scripts | Legacy alias for `POSTGRES_DATABASE_URL` |
| `MONGO_URI`, `MONGO_DB`, `RAW_COLLECTION_NAME` | Ingestion, Archiving | Raw ad storage |
| `PARSER_API_URL`, `PARSER_API_KEY` | Ingestion | External parser API |
| `ELASTICSEARCH_URL`, `PROCESSED_INDEX` | Ingestion, Matching, Pipeline, scripts | Processed ad index |
| `ELASTICSEARCH_API_KEY`, `ELASTICSEARCH_USERNAME`, `ELASTICSEARCH_PASSWORD` | Ingestion, Matching, Pipeline, scripts | Optional Elasticsearch auth |
| `MATCHING_BATCH_SIZE` | Matching | Documents processed per batch (clamped to 500–1000) |
| `MATCHING_SITES` | Matching | Optional comma-separated site filter (default: all sites with cursors) |
| `MATCHING_MIN_SCORE` | Matching | Minimum duplicate score to link listings (default `0.7`) |
| `MATCHING_TIME_WINDOW_DAYS` | Matching | Days to look back for duplicate candidates (default `5`) |
| `MATCHING_PARSER_LAG_DAYS` | Matching | Parser lag buffer in days (default `3`) |
| `MATCHING_PRICE_TOLERANCE` | Matching | Relative price tolerance (default `0.10`) |
| `MATCHING_MILEAGE_TOLERANCE` | Matching | Relative mileage tolerance (default `0.05`) |
| `MATCHING_MAX_RESULTS` | Matching | Max ES hits per duplicate query (default `200`, max `500`) |
| `ARCHIVE_RETENTION_DAYS` | Archiving | Days to keep raw ads in MongoDB before archiving (default `60`) |
| `ARCHIVE_BATCH_SIZE` | Archiving | Documents per archive batch (clamped to 1000–5000) |
| `S3_BUCKET`, `S3_PREFIX`, `S3_ENDPOINT` | Archiving | Object storage target (Yandex Object Storage by default) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Archiving | S3-compatible credentials |

Defaults and validators live in each service's `core/config.py` (for example `app/services/matching_service/core/config.py`).

## Utility scripts

| Script | Purpose |
|--------|---------|
| `scripts/backfill_matcher.py` | One-time historical duplicate-chain rebuild (see [Matching service vs backfill](#matching-service-vs-scriptsbackfill_matcherpy)); resumable, clears existing links first |
| `scripts/validate_matcher.py` | Read-only sample validation of matcher quality on live Elasticsearch data |
| `scripts/create_validation_index.py` | Create a validation Elasticsearch index |
| `scripts/matching_scenario/` | Controlled matching dry-run scenarios (see docs in that folder) |

## Tests

```bash
uv run pytest
```

Integration tests expect local MongoDB, Elasticsearch, and PostgreSQL instances. See `tests/integration/` for setup details used in each test suite.

## Project structure

```
app/
  clients/          # Storage and HTTP clients
  database/         # SQLAlchemy models, session factory, schema bootstrap
  repositories/     # Data access per storage backend
  schemas/          # Pydantic DTOs (raw and processed ads)
  services/         # Service entrypoints and business logic
    ingestion_service/
    matching_service/
    archiving_service/
    pipeline_runner/
    processing_service/
  uow/              # Unit of Work for transactional writes
scripts/            # Backfill, validation, and scenario tools
tests/              # Unit and integration tests
```
