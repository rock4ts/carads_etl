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

PostgreSQL state tables (`upload_timestamps`, `marker_timestamps`, `archive_batches`, `backfill_matcher_states`) are created automatically on first service start.

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

All services read from a `.env` file in the project root. See `.env.example` for the common variables.

| Variable | Used by | Description |
|----------|---------|-------------|
| `PARSER_API_URL`, `PARSER_API_KEY` | Ingestion | External parser API |
| `MONGO_URI`, `MONGO_DB`, `RAW_COLLECTION_NAME` | Ingestion, Archiving | Raw ad storage |
| `ELASTICSEARCH_URL`, `PROCESSED_INDEX` | Ingestion, Matching, Pipeline | Processed ad index |
| `POSTGRES_DATABASE_URL` | All services | Pipeline state |
| `MATCHING_BATCH_SIZE`, `MATCHING_SITES` | Matching | Batch size and optional site filter |
| `ARCHIVE_RETENTION_DAYS`, `ARCHIVE_BATCH_SIZE` | Archiving | Retention and batch size |
| `S3_BUCKET`, `S3_PREFIX`, `S3_ENDPOINT` | Archiving | Object storage target |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Archiving | S3 credentials |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID` | All services | Optional progress/critical alerts |
| `LOG_LEVEL` | All services | Logging verbosity |

Archiving-specific variables (`ARCHIVE_*`, `S3_*`, `AWS_*`) are not in `.env.example` but follow the defaults documented in `app/services/archiving_service/core/config.py`.

## Utility scripts

| Script | Purpose |
|--------|---------|
| `scripts/backfill_matcher.py` | One-time backfill of duplicate chains over existing Elasticsearch data |
| `scripts/validate_matcher.py` | Validate matching results against a reference index |
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
  database/         # SQLAlchemy models and session factory
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
