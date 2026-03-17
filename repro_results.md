# AR-000113790: LakebaseForeachWriter Latency Bug — Reproduction & Fix Verification

## Summary

Reproduced the `batch_interval_ms` latency bug using a **Spark Structured Streaming pipeline** writing to a real Lakebase Postgres instance, then verified the fix resolves it.

**Root cause**: The `_worker()` inner dequeue loop never checks `_time_to_flush()`. Under sustained load the queue is never empty, so the loop runs continuously until `batch_size` is reached. The `batch_interval_ms` time-based flush is effectively dead code.

**Fix**: Added `if self.batch and self._time_to_flush(): break` at the top of the inner dequeue loop, plus reduced dequeue timeout from 10ms to 1ms.

---

## DABs Bundle Jobs

Two Databricks jobs running the same notebook (`notebooks/rtm_latency_test.py`) with different `variant` parameters:

| Job | URL | Variant |
|-----|-----|---------|
| RTM Latency — BUGGY writer | [Job 481222725466898](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/481222725466898) | Original `_worker()` |
| RTM Latency — FIXED writer | [Job 534716171850219](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/534716171850219) | Patched `_worker()` |

### Final successful runs

| Job | Run URL |
|-----|---------|
| BUGGY | [Run 946858778815668](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/481222725466898/run/946858778815668) |
| FIXED | [Run 198462674193437](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/534716171850219/run/198462674193437) |

---

## Lakebase Instance

- **Project**: `ar113790-latency-repro` (Autoscaling tier)
- **Endpoint host**: `ep-lingering-feather-d1h9z1nl.database.us-west-2.cloud.databricks.com`
- **Database**: `databricks_postgres`
- **Table**: `rtm_latency_test`
- **Workspace**: e2-demo-field-eng (DEFAULT profile)

### Table Schema

```sql
CREATE TABLE rtm_latency_test (
    id BIGINT,                                   -- rate source row ID
    pipeline_ts TIMESTAMP,                       -- when Spark generated the row
    variant TEXT,                                 -- 'BUGGY' or 'FIXED'
    sink_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- when committed to Lakebase
);
```

The key metric is `sink_ts - pipeline_ts` = end-to-end write latency.

---

## Streaming Pipeline Configuration

- **Source**: `spark.readStream.format("rate").option("rowsPerSecond", 200)`
- **Sink**: `.writeStream.foreach(writer).trigger(processingTime="30 seconds")`
- **Duration**: 4 minutes (8 microbatches)
- **batch_size**: 1,000,000 (effectively infinite — never reached)
- **batch_interval_ms**: 1,000ms (1 second)
- **Cluster**: Single-node `m5.large`, DBR 16.4, auto-terminate 10 min

### How the bug manifests in streaming

Each 30-second microbatch contains ~6,000 rows. Spark calls `process()` for each row; a 5ms pacing sleep simulates RTM's continuous row arrival (keeping the queue populated). During the ~30-second processing window:

- **BUGGY**: The inner dequeue loop never checks time. With items always available in the queue (arriving every 5ms, well under the 10ms `get` timeout), the loop runs continuously. **No time-based flushes occur.** All rows flush in one burst at `close()` (end of microbatch).
- **FIXED**: The inner loop checks `_time_to_flush()` every iteration. After 1 second, it breaks out and flushes ~200 rows. This repeats ~30 times per microbatch, producing **steady 1-second flush cadence**.

---

## Results

### Key Metrics

| Metric | BUGGY (original) | FIXED (patched) |
|--------|-------------------|------------------|
| Total rows | 43,200 | 42,800 |
| **Distinct sink seconds** | **44** | **106** |
| Avg latency (pipeline→sink) | 25.42s | 23.73s |
| P50 latency | 25.60s | 23.95s |
| **P99 latency** | **37.11s** | **32.19s** |
| **Max latency** | **39.36s** | **32.77s** |
| Avg gap between arrivals | **5.0s** | **2.1s** |

### BUGGY: Per-second sink pattern (bursty, irregular)

```
16:47:39 |   604 rows | lat= 13.4s
16:47:40 |   545 rows | lat= 14.7s
16:47:42 |   496 rows | lat= 10.5s
16:47:43 |   555 rows | lat= 12.1s
              (19 second gap — no data arrives)
16:48:02 |   198 rows | lat= 27.2s
16:48:06 |  1744 rows | lat= 26.8s  ← burst
16:48:07 |   225 rows | lat= 22.5s
16:48:11 |  1779 rows | lat= 22.0s  ← burst
16:48:14 |  1054 rows | lat= 17.6s
              (17 second gap)
16:48:31 |   266 rows | lat= 31.6s
16:48:36 |  2046 rows | lat= 31.1s  ← burst
...
```

Data arrives in **large irregular bursts** separated by **multi-second gaps**. The bursts correspond to `close()` at microbatch boundaries.

### FIXED: Per-second sink pattern (steady, continuous)

```
16:47:07 |   166 rows | lat= 11.3s
16:47:08 |   375 rows | lat= 11.3s
16:47:09 |   373 rows | lat= 10.5s
16:47:10 |   385 rows | lat=  9.6s
16:47:11 |   501 rows | lat=  8.7s
              (brief pause between microbatches)
16:47:31 |   386 rows | lat= 26.7s
16:47:32 |   407 rows | lat= 25.8s
16:47:33 |   404 rows | lat= 24.8s
16:47:34 |   408 rows | lat= 23.8s
16:47:35 |   407 rows | lat= 22.8s
16:47:36 |   201 rows | lat= 21.8s
16:47:37 |   199 rows | lat= 21.8s
16:47:38 |   405 rows | lat= 20.9s
16:47:39 |   408 rows | lat= 19.9s
16:47:40 |   406 rows | lat= 18.9s
16:47:41 |   409 rows | lat= 17.9s
16:47:42 |   410 rows | lat= 16.9s
16:47:43 |   550 rows | lat= 15.6s
...
```

Data arrives in a **steady ~400 rows/sec every second** throughout each microbatch. The only gaps are brief pauses between microbatch transitions. Within each microbatch, **latency decreases steadily** (from ~31s down to ~18s) as the time-based flush catches up to the arriving rows.

---

## Lakebase Verification Queries

```bash
# Generate OAuth token
TOKEN=$(databricks postgres generate-database-credential \
  projects/ar113790-latency-repro/branches/production/endpoints/primary \
  -p DEFAULT -o json | jq -r '.token')

HOST="ep-lingering-feather-d1h9z1nl.database.us-west-2.cloud.databricks.com"
EMAIL="craig.lukasik@databricks.com"
DB="databricks_postgres"
```

### Query 1: Summary comparison
```sql
SELECT variant,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT date_trunc('second', sink_ts)) AS distinct_sink_seconds,
       ROUND(AVG(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 2) AS avg_latency_sec,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
           ORDER BY EXTRACT(EPOCH FROM (sink_ts - pipeline_ts))))::numeric, 2) AS p50_latency_sec,
       ROUND((PERCENTILE_CONT(0.99) WITHIN GROUP (
           ORDER BY EXTRACT(EPOCH FROM (sink_ts - pipeline_ts))))::numeric, 2) AS p99_latency_sec
FROM rtm_latency_test
GROUP BY 1
ORDER BY 1;
```

### Query 2: Per-second sink pattern (the key evidence)
```sql
SELECT variant,
       date_trunc('second', sink_ts) AS arrival_sec,
       COUNT(*) AS rows_arrived,
       ROUND(AVG(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 1) AS avg_latency
FROM rtm_latency_test
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Query 3: Gap analysis
```sql
WITH secs AS (
    SELECT variant, date_trunc('second', sink_ts) AS ts
    FROM rtm_latency_test
    GROUP BY 1, 2
),
gaps AS (
    SELECT variant, ts,
           EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (PARTITION BY variant ORDER BY ts))) AS gap_sec
    FROM secs
)
SELECT variant,
       MAX(gap_sec) AS max_gap_sec,
       ROUND(AVG(gap_sec)::numeric, 1) AS avg_gap_sec,
       COUNT(*) AS num_intervals
FROM gaps WHERE gap_sec IS NOT NULL
GROUP BY 1;
```

---

## Code Changes

**File**: `src/lakebase_foreachwriter/LakebaseForeachWriter.py`

### Fix 1 (Critical): Time check inside inner dequeue loop
```python
# BEFORE (buggy):
while len(self.batch) < self.batch_size:
    try:
        item = self.queue.get(timeout=0.01)  # 10ms
        self.batch.append(item)
    except queue.Empty:
        break

# AFTER (fixed):
while len(self.batch) < self.batch_size:
    if self.batch and self._time_to_flush():
        break  # ← NEW: break out to flush even when queue has items
    try:
        item = self.queue.get(timeout=0.001)  # 1ms (was 10ms)
        self.batch.append(item)
    except queue.Empty:
        break
```

### Fix 2: Thread-safe close() with longer timeout
- Join timeout increased from 5s to 30s
- Added `is_alive()` guard with error logging

### Unit Test
- Added `test_sustained_load_triggers_time_flush` regression test
- Verified test FAILS on original code (1 flush) and PASSES on fixed code (3+ flushes)

---

## Auth Lessons Learned

- **PATs do NOT work** for Lakebase auth — rejected as "not a valid JWT"
- **Notebook context tokens** (`dbutils.notebook...apiToken`) also don't work
- **Correct approach**: Use `POST /api/2.0/postgres/credentials` via `WorkspaceClient.api_client.do()` to generate a proper OAuth JWT
- The CLI equivalent is `databricks postgres generate-database-credential`

---

## Cleanup

```bash
# Delete the Lakebase project (and all data)
databricks postgres delete-project projects/ar113790-latency-repro -p DEFAULT

# Destroy the DABs bundle (jobs + uploaded files)
databricks bundle destroy --profile DEFAULT
```
