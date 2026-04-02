# LakebaseForeachWriter

A PySpark `ForeachWriter` for sending Structured Streaming rows to an existing Lakebase/Postgres table. It is optimized for Databricks [real-time mode](https://www.databricks.com/dataaisummit/session/real-time-mode-technical-deep-dive-how-we-built-sub-300-millisecond), but also works with standard micro-batches.

The writer buffers rows in memory, flushes on `batch_size` or `batch_interval_ms`, retries transient write failures, and can resolve a Lakebase read-write endpoint from `lakebase_name` via the Databricks SDK. It supports three write modes:

- `insert`: append rows with `INSERT`
- `upsert`: `INSERT ... ON CONFLICT DO UPDATE`
- `bulk-insert`: high-throughput PostgreSQL `COPY`

## Prerequisites and Compatibility

- Python 3.12+
- A Databricks runtime, or another environment where `pyspark` is already available
- `psycopg` for database writes
- `databricks-sdk` only if you use `lakebase_name` instead of passing `host` directly
- An existing target table with a schema that matches the DataFrame columns

This writer does not create tables or manage schema migrations.

Supported Spark types:

- Primitive scalar types such as `string`, `integer`, `long`, `float`, `double`, `boolean`, `date`, `timestamp`, and `decimal`
- Arrays of primitive scalar types

Unsupported Spark types:

- `struct`
- `map`
- Arrays containing `struct` or `map`

If you need to write complex columns, convert them to JSON or text before using this writer.

`databricks-connect` is used by this repo's local integration tests so they can create a `DatabricksSession`. The writer class itself does not call Databricks Connect at runtime, but the repo's dev toolchain is currently aligned to Python 3.12 because that is what the pinned Databricks Connect version supports.

## Installation

Build a wheel locally in this repo:

```bash
uv build
python -m pip install dist/lakebase_foreachwriter-*.whl
```

For local development in this repo:

```bash
uv sync --dev
```

GitHub distribution paths:

- Every CI run uploads `dist/*` as a GitHub Actions artifact.
- Tagged releases attach the wheel and source tarball to the GitHub Release.

So the intended paths are: build locally with `uv build`, download from a CI artifact, or download from a tagged GitHub Release.

If you prefer vendoring the source into a notebook or repo instead of installing a wheel, you can copy [src/lakebase_foreachwriter/LakebaseForeachWriter.py](src/lakebase_foreachwriter/LakebaseForeachWriter.py).

## Usage

The examples below assume:

```python
from lakebase_foreachwriter import LakebaseForeachWriter

streaming_df = ...
LAKEBASE_USER = "your-username"
LAKEBASE_PASSWORD = "your-password"
LAKEBASE_HOST = "your-lakebase.dns.databricks.com"
LAKEBASE_NAME = "your-lakebase-name"
```

### Example 1: `insert` mode with direct `host`

`insert` is the default mode. It appends every row it receives.

```python
writer = LakebaseForeachWriter(
    username=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD,
    table="public.events",
    df=streaming_df,
    host=LAKEBASE_HOST,
    mode="insert",
    batch_size=1000,
    batch_interval_ms=100,
)

query = (
    streaming_df.writeStream
    .foreach(writer)
    .option("checkpointLocation", "/tmp/spark_checkpoints/insert_example")
    .start()
)
```

### Example 2: `upsert` mode with `lakebase_name`

`upsert` is the safer choice when the stream can replay rows and you have stable primary keys.

```python
writer = LakebaseForeachWriter(
    username=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD,
    table="public.events",
    df=streaming_df,
    lakebase_name=LAKEBASE_NAME,
    mode="upsert",
    primary_keys=["id"],
    batch_size=1000,
    batch_interval_ms=100,
)

query = (
    streaming_df.writeStream
    .foreach(writer)
    .option("checkpointLocation", "/tmp/spark_checkpoints/upsert_example")
    .start()
)
```

### Example 3: `bulk-insert` mode

`bulk-insert` uses PostgreSQL `COPY` and is the highest-throughput option when you only need append behavior.

```python
writer = LakebaseForeachWriter(
    username=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD,
    table="public.events",
    df=streaming_df,
    host=LAKEBASE_HOST,
    mode="bulk-insert",
    batch_size=10000,
    batch_interval_ms=250,
)

query = (
    streaming_df.writeStream
    .foreach(writer)
    .option("checkpointLocation", "/tmp/spark_checkpoints/bulk_insert_example")
    .start()
)
```

## Key Parameters

- `username`, `password`: Database credentials. For production, prefer a dedicated database role or service principal-style account rather than an individual user.
- `table`: Target table name. `schema.table` is supported.
- `df`: The DataFrame whose schema is used to build SQL and validate supported types.
- `host`: Direct read-write DNS name for the target Lakebase/Postgres endpoint.
- `lakebase_name`: Lakebase instance name to resolve through the Databricks SDK. Provide this only when you want the writer to discover `host` for you.
- `sslmode`: Passed through to `psycopg`. Defaults to `require`. When connecting to local Postgres on `localhost` or `127.0.0.1`, the writer defaults to `disable`.
- `mode`: One of `insert`, `upsert`, or `bulk-insert`.
- `primary_keys`: Required for `upsert`. These columns define the `ON CONFLICT` target.
- `batch_size`: Flush after this many rows.
- `batch_interval_ms`: Flush after this amount of time even if the batch is not full.
- `max_queue_size`: In-memory queue size between Spark's `process()` calls and the background flush thread.
- `max_retries`: Number of flush retries after the initial attempt.
- `retry_base_delay_s`: Base delay for exponential backoff between retries.

You must provide either `host` or `lakebase_name`.

## Semantics and Caveats

- The writer expects the target table to already exist.
- The sink behaves effectively as at-least-once. On replay, restart, or retry, `insert` can produce duplicates.
- `upsert` is the safer mode when you need idempotent outcomes and have stable primary keys.
- `bulk-insert` maximizes throughput, but it is still append-only. It does not deduplicate rows.
- If you pass `host`, the writer does not need to call Databricks APIs. If you pass `lakebase_name`, the host lookup is cached for the life of the process.
- The writer does not support native OAuth token refresh for database credentials.

## Tuning

- Lower `batch_interval_ms` when end-to-end latency matters more than raw throughput.
- Raise `batch_size` when throughput matters more than per-row latency.
- Use `bulk-insert` for the highest insert throughput.
- Raise `max_queue_size` if Spark can temporarily outpace the database and you want more buffer before backpressure shows up in `process()`.
- Increase `max_retries` or `retry_base_delay_s` if your network path is noisy and brief reconnects are common.

## Operational Notes

Using a dedicated database login is recommended because the writer currently expects a stable username/password pair. For example:

```sql
CREATE ROLE "serviceprincipal"
  WITH LOGIN
       PASSWORD 'PASSWORD';
```

An alternative design is `foreachBatch` plus Spark's batch JDBC writer. That can work well, but it generally has higher latency than using this `ForeachWriter` directly, and it does not give you the same built-in `upsert` and `bulk-insert` modes.

## Running Tests

Set up the repo for local development:

```bash
uv sync --dev
```

Unit tests:

```bash
uv run pytest tests/test_foreachwriter.py -v
```

Integration tests:

```bash
uv run pytest tests/test_integration.py -v
```

The integration tests read a local `.env` file automatically. Set:

```bash
LAKEBASE_WRITER_HOST=your-lakebase.dns.databricks.com
LAKEBASE_WRITER_USER=your-username
LAKEBASE_WRITER_PASSWORD=your-password
# Optional in the current integration tests
LAKEBASE_WRITER_LAKEBASE_NAME=your-lakebase-name
```

If `LAKEBASE_WRITER_HOST`, `LAKEBASE_WRITER_USER`, or `LAKEBASE_WRITER_PASSWORD` are not set, the integration test module is skipped.

Never commit your `.env` file.
