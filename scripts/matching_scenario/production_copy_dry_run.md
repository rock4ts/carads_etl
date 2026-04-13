# Safe Matching Worker Dry Run

Run matching against a safe copy of the production-like index, not against production data.

## Guardrails

- Use source index `carads1_local`.
- Use target index `carads1_local_test`.
- Do not set target index equal to source index.
- Do not skip the reset step for `predecessor_id` / `successor_id`.
- Keep execution sequential (no parallel workers in this workflow).

## What the script does

`scripts/matching_scenario/run_matching_dry_run.py` performs:

1. Reads source index mapping/settings from `carads1_local`.
2. Recreates `carads1_local_test`.
3. Ensures:
   - `predecessor_id` is `keyword`
   - `successor_id` is `keyword`
4. Copies all documents with `_reindex`.
5. Resets matching fields with `_update_by_query`:
   - removes `predecessor_id`
   - removes `successor_id`
6. Picks one site and an hour-based `offer_start` window.
7. Seeds timestamps in Postgres:
   - `marker_timestamps.timestamp = lower_bound`
   - `upload_timestamps.timestamp = upper_bound`
8. Runs `python -m app.services.matching_service.main` with:
   - `PROCESSED_INDEX=carads1_local_test`
   - `MATCHING_SITES=<chosen site>`
9. Collects metrics:
   - `total_docs_processed`
   - `matches_found`
   - `claim_success`
   - `claim_failed`
10. Writes:
    - 20 matched pairs sample
    - 20 unmatched docs sample
    - chain verification report
11. Runs worker again and verifies idempotency (no link changes).

## Prerequisites

- Elasticsearch reachable via `ELASTICSEARCH_URL` (or `--es-url`).
- Postgres reachable via `POSTGRES_DATABASE_URL` (or `--postgres-url`).
- Python deps installed (`uv sync`).

## Run

```bash
uv run python scripts/run_matching_dry_run.py \
  --es-url http://localhost:19200 \
  --source-index carads1_local \
  --target-index carads1_local_test \
  --postgres-url postgresql+psycopg://postgres:postgres@localhost:5432/car_intel

# Or as module:
uv run python -m scripts.matching_scenario.run_matching_dry_run \
  --es-url http://localhost:19200 \
  --source-index carads1_local \
  --target-index carads1_local_test \
  --postgres-url postgresql+psycopg://postgres:postgres@localhost:5432/car_intel
```

## Optional arguments

- `--site-name <site>`: force a specific site instead of auto-picking.
- `--window-hours 24`: window size in hours.
- `--sample-size 20`: number of matched/unmatched samples.
- `--worker-module app.services.matching_service.main`: worker module path.
- `--reindex-timeout-seconds 7200`: max wait for `_reindex` task.
- `--reindex-poll-seconds 5`: poll interval for `_reindex`.
- `--reset-timeout-seconds 7200`: max wait for reset `_update_by_query` task.
- `--reset-poll-seconds 5`: poll interval for reset.
- `--output-dir <dir>`: custom artifacts directory.

Reset behavior:

- If no documents in target index have both `predecessor_id` and `successor_id`, reset is skipped automatically.

## Output artifacts

By default, artifacts are written to:

`artifacts/matching_dry_run/<timestamp>/`

Files:

- `run_summary.json`
- `metrics.json`
- `matched_pairs_sample.json`
- `unmatched_docs_sample.json`
- `chain_verification.json`
- `idempotency.json`
- `worker_first_run.log`
- `worker_second_run.log`

If `chain_verification.json` is invalid or idempotency fails, the script exits with an error.
