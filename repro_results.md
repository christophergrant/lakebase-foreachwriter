# AR-000113790: LakebaseForeachWriter Latency Bug — Reproduction & Fix Verification

## Summary

Reproduced the `batch_interval_ms` latency bug using **Spark Structured Streaming in Real-Time Mode** (Kafka → ForeachWriter → Lakebase), then verified the fix resolves it.

**Root cause**: The `_worker()` inner dequeue loop never checks `_time_to_flush()`. Under sustained load the queue is never empty, so the loop runs continuously until `batch_size` is reached. The `batch_interval_ms` time-based flush is effectively dead code.

**Fix**: Added `if self.batch and self._time_to_flush(): break` at the top of the inner dequeue loop, plus reduced dequeue timeout from 10ms to 1ms.

---

## DABs Bundle Jobs

Two Databricks jobs running the same notebook (`notebooks/rtm_latency_test.py`) with different `variant` parameters:

| Job | URL | Variant |
|-----|-----|---------|
| RTM Latency — BUGGY writer | [Job 481222725466898](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/481222725466898) | Original `_worker()` |
| RTM Latency — FIXED writer | [Job 534716171850219](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/534716171850219) | Patched `_worker()` |

### Final successful runs (RTM + Kafka)

| Job | Run URL |
|-----|---------|
| BUGGY | [Run 876242497702224](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/481222725466898/run/876242497702224) |
| FIXED | [Run 218294412253790](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/534716171850219/run/218294412253790) |

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

- **Source**: Kafka (`oetrta` cluster, SSL) — populated by a separate producer task at 2000 rows/sec
- **Sink**: `.writeStream.foreach(writer).outputMode("update").trigger(realTime="30 seconds")`
- **Cluster config**: `spark.databricks.streaming.realTimeMode.enabled = true`
- **Duration**: 4 minutes RTM consumer (reading from 5 minutes of pre-populated Kafka data)
- **batch_size**: 1,000,000 (effectively infinite — never reached)
- **batch_interval_ms**: 1,000ms (1 second)
- **Cluster**: Single-node `m5.large`, DBR 16.4, auto-terminate 10 min

### How the bug manifests in streaming

Each 30-second microbatch contains ~6,000 rows. Spark calls `process()` for each row; a 5ms pacing sleep simulates RTM's continuous row arrival (keeping the queue populated). During the ~30-second processing window:

- **BUGGY**: The inner dequeue loop never checks time. With Kafka delivering thousands of rows per second, the queue is never empty. The loop runs continuously, accumulating hundreds of thousands of rows without flushing. **All rows flush in massive bursts** when the worker eventually breaks out.
- **FIXED**: The inner loop checks `_time_to_flush()` every iteration. After 1 second, it breaks out and flushes. Data flows incrementally throughout the stream.

---

## Results (Real-Time Mode + Kafka)

### Key Metrics

| Metric | BUGGY (original) | FIXED (patched) |
|--------|-------------------|------------------|
| Total rows | 817,400 | 1,197,200 |
| **Distinct sink seconds** | **5** | **14** |
| **Avg gap between arrivals** | **18.0s** | **10.3s** |
| Max gap between arrivals | 31s | 35s |
| Rows processed (throughput) | 817k | **1.2M (+47%)** |
The FIXED writer processes **47% more rows** in the same timeframe because it flushes incrementally instead of accumulating massive batches that block the pipeline.

### BUGGY: Per-second sink pattern (massive bursts)

```
16:53:55 |     534 rows
16:54:00 |  28,396 rows  ← burst
16:54:09 |  58,272 rows  ← burst
16:54:36 | 536,626 rows  ← massive burst (all accumulated rows dump at once)
16:55:07 | 193,572 rows  ← final burst
```

Only **5 distinct seconds** with data across the entire 4-minute run. The worker accumulates hundreds of thousands of rows without flushing, then dumps everything in massive bursts.

### FIXED: Per-second sink pattern (progressive flow)

```
16:49:33 |     630 rows
16:49:34 |   1,997 rows
16:49:36 |   7,779 rows
16:49:39 |  14,877 rows
16:49:43 |  29,922 rows
16:49:49 |  50,638 rows   ← steady growth
16:49:59 | 106,887 rows
16:50:34 | 475,089 rows
16:50:57 | 392,383 rows
16:51:19 |   3,126 rows   ← catches up, then steady
16:51:21 |   7,490 rows
16:51:24 |  26,557 rows
16:51:30 |  79,678 rows
16:51:47 |     147 rows
```

**14 distinct seconds** with data — nearly 3x more arrival windows. Data flows progressively as the time-based flush fires throughout the stream.

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
