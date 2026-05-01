# Changelog

All notable changes to this project are documented in this file.

## Unreleased

- No unreleased changes yet.

## 0.1.0 - 2026-05-01

- Added `LakebaseForeachWriter` for writing PySpark Structured Streaming rows to Lakebase/Postgres.
- Added `insert`, `upsert`, and `bulk-insert` write modes.
- Added batching controls, bounded queue backpressure, flush retries, and reconnect handling.
- Added Lakebase host resolution by `lakebase_name` for provisioned and autoscaling Lakebase instances.
- Added OAuth credential provider support for Databricks SDK-generated database credentials.
- Added versioned upsert support with optional coalescing of selected incoming `NULL` values.
- Added executor-visible logging for writer lifecycle, flush, retry, and close events.
- Added unit tests, local Postgres tests, integration tests, CI, and GitHub Release packaging.
