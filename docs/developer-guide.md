# Developer Guide

## Common Workflows

### Run end-to-end pipeline (A2)

```bash
python run_pipeline.py --records 100
```

### Terminal CRUD query interface

```bash
python -m src.cli query --op read --payload '{"fields":["username"],"limit":5}'
```

### Run dashboard backend

```bash
python -m dashboard.api_server
```

### Run concurrency and ACID tests

```bash
python test_concurrency.py
python test_concurrency_e2e.py
python test_logical_reconstructor.py
```

## Configuration

Runtime configuration is loaded from `.env` via `src/config.py`.

Important environment variables:

- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- `MONGO_HOST`, `MONGO_PORT`, `MONGO_USER`, `MONGO_PASSWORD`, `MONGO_DATABASE`
- `BUFFER_SIZE`, `BUFFER_TIMEOUT_SECONDS`
- `METADATA_DIR`
- `DATA_STREAM_URL`

## Generated Metadata Artifacts

The pipeline writes planning/runtime artifacts under `metadata/`:

- `schema.json`
- `sql_plan.json`
- `mongo_plan.json`
- `field_locations.json`
- `query_history.jsonl`
- `state.json`

## Notes

- Keep Docker databases running before executing pipeline or API workflows.
- For frontend in Docker, use compose `frontend` profile.
- For load testing in Docker, use compose `loadtest` profile.
