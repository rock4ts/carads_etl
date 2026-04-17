# ETL Monorepo

Minimal Python 3.12 ETL scaffold using `uv` for dependency management.

## Structure

```text
etl/
  app/
    services/
      ingestion_service/
      processing_service/
      matching_service/
      archiving_service/
    shared/
      models/
      clients/
      utils/
      config/
    infra/
      docker/
      mongodb/
      elasticsearch/
  scripts/
  tests/
  .env.example
  .python-version
  pyproject.toml
  README.md
```

## Requirements

- Python 3.12
- `uv`

## Setup

Create this index on the raw ads collection (e.g. in `mongosh`):

```javascript
db.raw_ads.createIndex({
  source: 1,
  "payload.parapi_unique_id": 1,
  ingested_at: -1
})
```

## Getting Started

```bash
uv sync
cp .env.example .env
uv run python -m app.services.ingestion_service.main
```

Replace `ingestion_service` with `processing_service`, `matching_service`, or `archiving_service` to run a different service entrypoint.
