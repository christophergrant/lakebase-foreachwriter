# Databricks notebook source
# MAGIC %md
# MAGIC # LakebaseForeachWriter Latency Bug Reproduction (AR-000113790)
# MAGIC
# MAGIC **Bug**: Under sustained load, `_worker()` only flushes at `batch_size` boundaries
# MAGIC or when the queue empties — the `batch_interval_ms` time-based flush is dead code
# MAGIC because the inner dequeue loop never checks `_time_to_flush()`.
# MAGIC
# MAGIC **Test plan**: Run the same workload against two inline writer implementations:
# MAGIC 1. **BUGGY** — original `_worker()` (no time check in inner loop)
# MAGIC 2. **FIXED** — patched `_worker()` (breaks out of inner loop when `_time_to_flush()`)
# MAGIC
# MAGIC A monitor thread polls Lakebase to observe when rows actually arrive.

# COMMAND ----------

import logging
import queue
import threading
import time
from datetime import datetime, timezone

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("latency-repro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Lakebase Connection

# COMMAND ----------

LAKEBASE_PROJECT = "ar113790-latency-repro"
LAKEBASE_ENDPOINT = f"projects/{LAKEBASE_PROJECT}/branches/production/endpoints/primary"
TABLE_NAME = "latency_repro_test"

# Use WorkspaceClient to call the Lakebase credential API.
# Lakebase requires a JWT — PATs are rejected with "not a valid JWT encoding".
# The generate-database-credential API returns a proper OAuth JWT.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

LAKEBASE_USER = w.current_user.me().user_name

# Generate OAuth JWT for Lakebase via REST API
cred_response = w.api_client.do(
    "POST", "/api/2.0/postgres/credentials", body={"endpoint": LAKEBASE_ENDPOINT}
)
LAKEBASE_PASSWORD = cred_response["token"]

# Get endpoint host via REST API
endpoints_response = w.api_client.do(
    "GET",
    f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches/production/endpoints",
)
LAKEBASE_HOST = endpoints_response["endpoints"][0]["status"]["hosts"]["host"]

logger.info(f"Lakebase host: {LAKEBASE_HOST}")
logger.info(f"Lakebase user: {LAKEBASE_USER}")
logger.info(f"OAuth token length: {len(LAKEBASE_PASSWORD)}")

conn_params = {
    "host": LAKEBASE_HOST,
    "port": 5432,
    "dbname": "databricks_postgres",
    "user": LAKEBASE_USER,
    "password": LAKEBASE_PASSWORD,
    "sslmode": "require",
}

# Verify connectivity
with psycopg.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        logger.info("Lakebase connectivity verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Table
# MAGIC
# MAGIC The `_server_ts` column uses `DEFAULT CURRENT_TIMESTAMP` so we can query
# MAGIC Lakebase after the test to see the actual server-side arrival times.

# COMMAND ----------

with psycopg.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        cur.execute(f"""
            CREATE TABLE {TABLE_NAME} (
                id INTEGER PRIMARY KEY,
                value TEXT,
                test_variant TEXT,
                client_ts DOUBLE PRECISION,
                _server_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    logger.info(f"Created table {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inline Writer Implementations
# MAGIC
# MAGIC Two variants: **BUGGY** (original code) and **FIXED** (with time check in inner loop).

# COMMAND ----------


class _BaseWriter:
    """Shared writer logic — subclasses only override _worker()."""

    def __init__(self, conn_params, table, columns, batch_size, batch_interval_ms):
        self.conn_params = conn_params
        self.table = table
        self.columns = columns
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        self.sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    def open(self, partition_id, epoch_id):
        self.partition_id = partition_id
        self.epoch_id = epoch_id
        self.conn = psycopg.connect(**self.conn_params)
        self.queue = queue.SimpleQueue()
        self.stop_event = threading.Event()
        self.batch = []
        self.last_flush = time.time()
        self.worker_error = None
        self.flush_count = 0
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()
        return True

    def process(self, row):
        if self.worker_error:
            raise Exception(f"Worker failed: {self.worker_error}")
        self.queue.put(tuple(row))

    def close(self, error=None):
        if self.stop_event:
            self.stop_event.set()
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=30.0)
        self._flush_remaining()
        if self.conn:
            self.conn.close()

    def _worker(self):
        raise NotImplementedError

    def _flush_batch(self):
        if not self.batch:
            return
        with self.conn.cursor() as cur:
            cur.executemany(self.sql, self.batch)
        self.conn.commit()
        n = len(self.batch)
        self.batch = []
        self.last_flush = time.time()
        self.flush_count += 1
        logger.info(f"  [flush #{self.flush_count}] {n} rows")

    def _flush_remaining(self):
        while True:
            try:
                self.batch.append(self.queue.get_nowait())
            except queue.Empty:
                break
        if self.batch:
            self._flush_batch()

    def _time_to_flush(self):
        return (time.time() - self.last_flush) * 1000 >= self.batch_interval_ms


class BuggyWriter(_BaseWriter):
    """Original _worker() — inner loop does NOT check _time_to_flush()."""

    def _worker(self):
        while not self.stop_event.is_set():
            try:
                while len(self.batch) < self.batch_size:
                    try:
                        item = self.queue.get(timeout=0.01)  # 10ms timeout
                        self.batch.append(item)
                    except queue.Empty:
                        break

                should_flush = len(self.batch) >= self.batch_size or (
                    self.batch and self._time_to_flush()
                )
                if should_flush:
                    self._flush_batch()

                time.sleep(0.0001)
            except Exception as e:
                self.worker_error = str(e)
                break


class FixedWriter(_BaseWriter):
    """Patched _worker() — inner loop breaks out when _time_to_flush()."""

    def _worker(self):
        while not self.stop_event.is_set():
            try:
                while len(self.batch) < self.batch_size:
                    if self.batch and self._time_to_flush():
                        break
                    try:
                        item = self.queue.get(timeout=0.001)  # 1ms timeout
                        self.batch.append(item)
                    except queue.Empty:
                        break

                should_flush = len(self.batch) >= self.batch_size or (
                    self.batch and self._time_to_flush()
                )
                if should_flush:
                    self._flush_batch()

                time.sleep(0.0001)
            except Exception as e:
                self.worker_error = str(e)
                break


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Harness

# COMMAND ----------


def run_test(writer, test_variant, duration_sec=10, rows_per_sec=2000):
    """
    Feed rows at a sustained rate and monitor Lakebase for arrival times.

    Key: rows_per_sec must be high enough that the queue is never empty
    for the buggy writer's 10ms dequeue timeout. At 2000 rows/sec, a new
    row arrives every 0.5ms — well under the 10ms timeout — so the inner
    dequeue loop never hits queue.Empty and the bug manifests.
    """
    arrival_timeline = []
    stop_monitor = threading.Event()

    def monitor():
        """Poll Lakebase every 200ms to see when rows arrive."""
        with psycopg.connect(**conn_params) as mon_conn:
            while not stop_monitor.is_set():
                try:
                    with mon_conn.cursor() as cur:
                        cur.execute(
                            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE test_variant = %s",
                            (test_variant,),
                        )
                        count = cur.fetchone()[0]
                        arrival_timeline.append((time.time(), count))
                except Exception:
                    pass
                time.sleep(0.2)

    # Clear previous data for this variant
    with psycopg.connect(**conn_params) as c:
        with c.cursor() as cur:
            cur.execute(
                f"DELETE FROM {TABLE_NAME} WHERE test_variant = %s", (test_variant,)
            )
        c.commit()

    # Start monitor
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()

    # Open writer
    writer.open(0, 0)

    # Feed rows at sustained rate — tight loop with minimal sleep
    # to keep the queue always populated
    sleep_interval = 1.0 / rows_per_sec
    start = time.time()
    row_id = 0
    while time.time() - start < duration_sec:
        writer.process((row_id, f"value_{row_id}", test_variant, time.time()))
        row_id += 1
        # Tight spin — at 2000/sec this is 0.5ms between rows,
        # well under the buggy writer's 10ms dequeue timeout
        if sleep_interval > 0.0001:
            time.sleep(sleep_interval)

    feed_done = time.time()
    logger.info(f"[{test_variant}] Fed {row_id} rows in {feed_done - start:.1f}s")

    # Close writer (triggers final flush)
    close_start = time.time()
    writer.close(None)
    close_end = time.time()

    # Let monitor catch final state
    time.sleep(1.0)
    stop_monitor.set()
    monitor_thread.join(timeout=2)

    # Analyze arrival timeline
    distinct_counts = []
    prev_count = -1
    for ts, count in arrival_timeline:
        if count != prev_count and count > 0:
            distinct_counts.append((ts - start, count))
            prev_count = count

    result = {
        "variant": test_variant,
        "rows_sent": row_id,
        "duration_sec": duration_sec,
        "close_ms": (close_end - close_start) * 1000,
        "worker_flushes": writer.flush_count,
        "monitor_batches": len(distinct_counts),
        "arrival_timeline": distinct_counts,
    }

    logger.info(f"\n{'=' * 60}")
    logger.info(f"[{test_variant}] RESULTS")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Rows sent:             {result['rows_sent']}")
    logger.info(f"  Worker flush count:    {result['worker_flushes']}")
    logger.info(f"  Monitor batch count:   {result['monitor_batches']}")
    logger.info(f"  close() duration:      {result['close_ms']:.0f}ms")
    logger.info(f"  Arrival timeline:")
    for i, (elapsed, count) in enumerate(distinct_counts):
        logger.info(
            f"    Batch {i + 1}: {count} rows visible at +{elapsed * 1000:.0f}ms"
        )

    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 1: BUGGY Writer (original code)

# COMMAND ----------

buggy = BuggyWriter(
    conn_params=conn_params,
    table=TABLE_NAME,
    columns=["id", "value", "test_variant", "client_ts"],
    batch_size=50000,  # Very large — never reached
    batch_interval_ms=500,  # 500ms flush interval (should trigger, but won't under sustained load)
)

# 2000 rows/sec for 10 seconds = 20k rows, queue always full
buggy_result = run_test(buggy, "BUGGY", duration_sec=10, rows_per_sec=2000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 2: FIXED Writer (patched code)

# COMMAND ----------

fixed = FixedWriter(
    conn_params=conn_params,
    table=TABLE_NAME,
    columns=["id", "value", "test_variant", "client_ts"],
    batch_size=50000,  # Same config as buggy
    batch_interval_ms=500,  # 500ms flush interval (should now trigger correctly)
)


# Offset IDs so they don't conflict with buggy run
class OffsetFixedWriter:
    def __init__(self, inner, offset):
        self._inner = inner
        self._offset = offset

    def open(self, p, e):
        return self._inner.open(p, e)

    def process(self, row):
        self._inner.process((row[0] + self._offset, row[1], row[2], row[3]))

    def close(self, e=None):
        self._inner.close(e)

    @property
    def flush_count(self):
        return self._inner.flush_count


fixed_wrapper = OffsetFixedWriter(fixed, offset=100000)
fixed_result = run_test(fixed_wrapper, "FIXED", duration_sec=10, rows_per_sec=2000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison & Verdict

# COMMAND ----------

print(f"\n{'=' * 70}")
print(f" COMPARISON: BUGGY vs FIXED")
print(f"{'=' * 70}")
print(f"{'Metric':<30} {'BUGGY':>15} {'FIXED':>15}")
print(f"{'-' * 60}")
print(
    f"{'Rows sent':<30} {buggy_result['rows_sent']:>15} {fixed_result['rows_sent']:>15}"
)
print(
    f"{'Worker flushes':<30} {buggy_result['worker_flushes']:>15} {fixed_result['worker_flushes']:>15}"
)
print(
    f"{'Monitor arrival batches':<30} {buggy_result['monitor_batches']:>15} {fixed_result['monitor_batches']:>15}"
)
print(
    f"{'close() duration (ms)':<30} {buggy_result['close_ms']:>15.0f} {fixed_result['close_ms']:>15.0f}"
)
print(f"{'=' * 70}")

buggy_ok = buggy_result["worker_flushes"] <= 2
fixed_ok = fixed_result["worker_flushes"] >= 5

if buggy_ok and fixed_ok:
    print("\n** BUG CONFIRMED AND FIX VERIFIED **")
    print(
        f"  BUGGY: Only {buggy_result['worker_flushes']} flush(es) — batch_interval_ms is dead code under sustained load"
    )
    print(
        f"  FIXED: {fixed_result['worker_flushes']} flushes — time-based flush works correctly"
    )
elif not buggy_ok:
    print("\n** UNEXPECTED: Buggy writer flushed more than expected **")
    print("  The bug may not reproduce under this load profile.")
else:
    print("\n** UNEXPECTED: Fixed writer did not flush enough **")
    print("  The fix may not be working correctly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase Queries for Post-Hoc Analysis
# MAGIC
# MAGIC Run these queries directly in Lakebase to see the server-side arrival pattern:
# MAGIC
# MAGIC ```sql
# MAGIC -- Per-second arrival counts by variant
# MAGIC SELECT test_variant,
# MAGIC        date_trunc('second', _server_ts) AS arrival_sec,
# MAGIC        COUNT(*) AS rows_arrived
# MAGIC FROM latency_repro_test
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2;
# MAGIC
# MAGIC -- Summary: spread of arrivals
# MAGIC SELECT test_variant,
# MAGIC        COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT date_trunc('second', _server_ts)) AS distinct_seconds,
# MAGIC        MIN(_server_ts) AS first_arrival,
# MAGIC        MAX(_server_ts) AS last_arrival,
# MAGIC        MAX(_server_ts) - MIN(_server_ts) AS arrival_span
# MAGIC FROM latency_repro_test
# MAGIC GROUP BY 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Analysis Queries

# COMMAND ----------

print("\n--- Per-variant arrival spread ---")
with psycopg.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT test_variant,
                   COUNT(*) AS total_rows,
                   COUNT(DISTINCT date_trunc('second', _server_ts)) AS distinct_seconds,
                   MIN(_server_ts) AS first_arrival,
                   MAX(_server_ts) AS last_arrival,
                   MAX(_server_ts) - MIN(_server_ts) AS arrival_span
            FROM {TABLE_NAME}
            GROUP BY 1
            ORDER BY 1
        """)
        rows = cur.fetchall()
        for row in rows:
            print(
                f"  {row[0]}: {row[1]} rows across {row[2]} distinct seconds, span={row[5]}"
            )

print("\n--- Per-second arrival counts ---")
with psycopg.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT test_variant,
                   date_trunc('second', _server_ts) AS arrival_sec,
                   COUNT(*) AS rows_arrived
            FROM {TABLE_NAME}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        rows = cur.fetchall()
        for row in rows:
            print(f"  {row[0]} | {row[1]} | {row[2]} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Uncomment the cell below to drop the test table and delete the temporary token.
# MAGIC Left commented so you can inspect the data in Lakebase after the run.

# COMMAND ----------

# with psycopg.connect(**conn_params) as conn:
#     with conn.cursor() as cur:
#         cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
#     conn.commit()
#     logger.info(f"Dropped table {TABLE_NAME}")
