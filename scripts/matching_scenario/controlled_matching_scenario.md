# Controlled Matching Worker Scenario

Run an end-to-end deterministic scenario for `app.services.matching_service.main`.

## Prerequisites

- Reachable Elasticsearch (example: `http://localhost:19200`)
- Reachable Postgres (example: `postgresql+psycopg://postgres:postgres@localhost:5432/car_intel`)
- Python environment with project dependencies installed

## What the script does

`scripts/run_controlled_matching_scenario.py` performs the full test workflow:

1. Recreates the test index from `index_example.json`
2. Ensures `predecessor_id` / `successor_id` mapping exists
3. Seeds 25 docs:
   - Perfect chain: `C -> A -> B`
   - Competing candidates: `C -> {A, B}` (one claim fails)
   - No-match document
   - Edge mismatch pair (engine/gearbox mismatch)
   - Additional filler docs
4. Seeds Postgres:
   - `marker_timestamps.timestamp = earliest offer_start`
   - `upload_timestamps.timestamp = latest offer_start`
5. Runs worker once and checks:
   - expected links
   - graph invariants
   - log evidence of claim successes/failures
6. Runs worker again and verifies idempotency (no link changes)

## Run
```bash

curl -X PUT "http://localhost:19200/_cluster/settings" \
  -H "Content-Type: application/json" \
  -d '{
    "transient": {
      "indices.id_field_data.enabled": true
    }
  }'

python -m scripts.matching_scenario.run_controlled_matching_scenario \
  --es-url http://localhost:19200 \
  --index carads1 \
  --postgres-url postgresql+psycopg://postgres:postgres@localhost:5432/car_intel \
  --site-name matching_controlled_test
```

## Useful options

- `--index-definition index_example.json` - source index mapping/settings file
- `--keep-index` - skip index recreation
- `--wait-seconds 30` - Elasticsearch readiness timeout
- `--worker-module app.services.matching_service.main` - worker module path

