# Databricks notebook source
# MAGIC %md
# MAGIC # RTM Latency Test — LakebaseForeachWriter (AR-000113790)
# MAGIC
# MAGIC Runs a Spark Structured Streaming pipeline in **Real-Time Mode** using
# MAGIC `foreach(writer)` to sink rows from a rate source into Lakebase.
# MAGIC Parameterized by `variant` (BUGGY or FIXED) to compare the original
# MAGIC and patched `_worker()` implementations.
# MAGIC
# MAGIC **Trigger**: `.trigger(realTime="30 seconds")` — rows flow continuously
# MAGIC through `process()` as they arrive; no microbatch buffering.
# MAGIC
# MAGIC **Table**: `rtm_latency_test` in Lakebase
# MAGIC - `pipeline_ts` — when Spark generated the row (rate source timestamp)
# MAGIC - `sink_ts` — when the row was committed to Lakebase (`DEFAULT CURRENT_TIMESTAMP`)
# MAGIC - `variant` — which writer implementation produced the row
# MAGIC
# MAGIC The delta `sink_ts - pipeline_ts` is the end-to-end write latency.

# COMMAND ----------

# Parameters
dbutils.widgets.text("variant", "FIXED", "Writer variant (BUGGY or FIXED)")
dbutils.widgets.text("duration_minutes", "3", "How long to stream")
dbutils.widgets.text("rows_per_second", "1000", "Rate source rows/sec")

VARIANT = dbutils.widgets.get("variant").upper()
DURATION_MINUTES = int(dbutils.widgets.get("duration_minutes"))
ROWS_PER_SECOND = int(dbutils.widgets.get("rows_per_second"))

assert VARIANT in ("BUGGY", "FIXED"), f"variant must be BUGGY or FIXED, got {VARIANT}"
print(
    f"Config: variant={VARIANT}, duration={DURATION_MINUTES}min, rate={ROWS_PER_SECOND}/sec"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase Auth

# COMMAND ----------

import logging
import queue
import threading
import time

import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rtm-latency")

LAKEBASE_PROJECT = "ar113790-latency-repro"
LAKEBASE_ENDPOINT = f"projects/{LAKEBASE_PROJECT}/branches/production/endpoints/primary"
TABLE_NAME = "rtm_latency_test"

w = WorkspaceClient()
LAKEBASE_USER = w.current_user.me().user_name

# Generate OAuth JWT via the Lakebase credential API
cred_response = w.api_client.do(
    "POST",
    "/api/2.0/postgres/credentials",
    body={"endpoint": LAKEBASE_ENDPOINT},
)
LAKEBASE_PASSWORD = cred_response["token"]

# Get endpoint host
endpoints_response = w.api_client.do(
    "GET",
    f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches/production/endpoints",
)
LAKEBASE_HOST = endpoints_response["endpoints"][0]["status"]["hosts"]["host"]

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
logger.info(f"Connected to Lakebase: {LAKEBASE_HOST}")

# Ensure table exists
with psycopg.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rtm_latency_test (
                id BIGINT,
                pipeline_ts TIMESTAMP,
                variant TEXT,
                sink_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Clear previous data for this variant
        cur.execute("DELETE FROM rtm_latency_test WHERE variant = %s", (VARIANT,))
    conn.commit()
logger.info(f"Cleared previous {VARIANT} data from rtm_latency_test")

# Clear stale checkpoints (consumer and producer)
for ckpt_path in [
    f"/tmp/rtm_latency_ckpt/{VARIANT.lower()}",
    f"/tmp/rtm_kafka_producer_ckpt/{VARIANT.lower()}",
]:
    try:
        dbutils.fs.rm(ckpt_path, recurse=True)
        logger.info(f"Cleared checkpoint: {ckpt_path}")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## ForeachWriter Implementations
# MAGIC
# MAGIC Both implement the PySpark `ForeachWriter` protocol (`open/process/close`).
# MAGIC The only difference is in `_worker()`:
# MAGIC - **BUGGY**: inner dequeue loop does NOT check `_time_to_flush()`
# MAGIC - **FIXED**: inner dequeue loop breaks out when `_time_to_flush()` is True

# COMMAND ----------


class _BaseForeachWriter:
    """
    Shared ForeachWriter logic. Subclasses override _worker() only.

    This mirrors the real LakebaseForeachWriter structure:
    - open(): connects to Lakebase, starts background worker thread
    - process(): enqueues rows (called by Spark for each Row)
    - close(): signals worker to stop, flushes remaining, closes connection
    - _worker(): background thread that dequeues and flushes batches
    """

    def __init__(
        self,
        conn_params,
        table,
        columns,
        variant,
        batch_size=50000,
        batch_interval_ms=500,
    ):
        self.conn_params = conn_params
        self.table = table
        self.columns = columns
        self.variant = variant
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms

        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        self.sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    def open(self, partition_id, epoch_id):
        """Called by Spark at the start of each partition/epoch."""
        try:
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
            logging.info(f"[{partition_id}|{epoch_id}] Opened {self.variant} writer")
            return True
        except Exception as e:
            logging.error(f"Failed to open writer: {e}")
            return False

    def process(self, row):
        """Called by Spark for each Row in the partition."""
        if self.worker_error:
            raise Exception(f"Worker failed: {self.worker_error}")
        # Extract values by column name from the Spark Row,
        # just like the real LakebaseForeachWriter does
        if isinstance(row, Row):
            row_data = tuple(row[col] for col in self.columns)
        else:
            row_data = tuple(row)
        self.queue.put(row_data)

    def close(self, error):
        """Called by Spark at the end of each partition/epoch."""
        try:
            if self.stop_event:
                self.stop_event.set()
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=30.0)
                if self.worker_thread.is_alive():
                    logging.error(
                        f"[{self.partition_id}|{self.epoch_id}] "
                        "Worker thread did not stop within 30s"
                    )
            self._flush_remaining()
        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
        logging.info(
            f"[{self.partition_id}|{self.epoch_id}] "
            f"Closed {self.variant} writer — {self.flush_count} flushes"
        )

    def _worker(self):
        raise NotImplementedError

    def _flush_batch(self):
        if not self.batch:
            return
        try:
            with self.conn.cursor() as cur:
                cur.executemany(self.sql, self.batch)
            self.conn.commit()
            n = len(self.batch)
            self.batch = []
            self.last_flush = time.time()
            self.flush_count += 1
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise

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


class BuggyForeachWriter(_BaseForeachWriter):
    """Original _worker() — inner loop does NOT check _time_to_flush()."""

    def _worker(self):
        while not self.stop_event.is_set():
            try:
                # BUG: This loop runs until batch_size or queue.Empty.
                # Under sustained load, queue is never empty, so this
                # loop runs for the entire epoch without ever breaking
                # out to check _time_to_flush().
                while len(self.batch) < self.batch_size:
                    try:
                        item = self.queue.get(timeout=0.01)  # 10ms
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
                logging.error(f"Worker error: {e}")
                self.worker_error = str(e)
                break


class FixedForeachWriter(_BaseForeachWriter):
    """Patched _worker() — inner loop breaks out when _time_to_flush()."""

    def _worker(self):
        while not self.stop_event.is_set():
            try:
                while len(self.batch) < self.batch_size:
                    # FIX: Check time INSIDE the inner loop so we break
                    # out to flush even when the queue has items.
                    if self.batch and self._time_to_flush():
                        break
                    try:
                        item = self.queue.get(timeout=0.001)  # 1ms (was 10ms)
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
                logging.error(f"Worker error: {e}")
                self.worker_error = str(e)
                break


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build RTM Streaming Consumer
# MAGIC
# MAGIC Reads from Kafka topic populated by the `populate_kafka` task.
# MAGIC Uses Real-Time Mode trigger so rows flow continuously through `process()`.

# COMMAND ----------

from pyspark.sql.functions import from_json, col as _col
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, StringType

KAFKA_BOOTSTRAP = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-tls")
KAFKA_TOPIC = f"ar113790-rtm-latency-{VARIANT.lower()}"

# Schema of the JSON payload written to Kafka by the populate_kafka task
kafka_value_schema = StructType([
    StructField("id", LongType()),
    StructField("pipeline_ts", TimestampType()),
    StructField("variant", StringType()),
])

# Read from Kafka and parse the JSON value
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", "SSL")
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

stream_df = (
    raw_stream
    .select(from_json(_col("value").cast("string"), kafka_value_schema).alias("data"))
    .select("data.id", "data.pipeline_ts", "data.variant")
)

print(f"Stream schema: {stream_df.schema.simpleString()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Writer & Start Streaming

# COMMAND ----------

# Columns written to Lakebase (must match DataFrame column order)
WRITER_COLUMNS = ["id", "pipeline_ts", "variant"]

if VARIANT == "BUGGY":
    writer = BuggyForeachWriter(
        conn_params=conn_params,
        table=TABLE_NAME,
        columns=WRITER_COLUMNS,
        variant=VARIANT,
        batch_size=1000000,  # Effectively infinite — never reached
        batch_interval_ms=1000,  # 1s — should flush once per second
    )
else:
    writer = FixedForeachWriter(
        conn_params=conn_params,
        table=TABLE_NAME,
        columns=WRITER_COLUMNS,
        variant=VARIANT,
        batch_size=1000000,  # Same as buggy
        batch_interval_ms=1000,
    )

print(
    f"Using {VARIANT} writer: batch_size={writer.batch_size}, "
    f"batch_interval_ms={writer.batch_interval_ms}"
)

# Real-Time Mode: rows flow continuously through process() as they arrive
# from the source (no microbatch buffering). The checkpoint interval controls
# how often progress is committed, not when data is processed.
#
# With RTM, the ForeachWriter's open() is called once per partition, process()
# is called continuously, and close() only at query stop. This means:
#   BUGGY: rows accumulate indefinitely — no time-based flush ever fires
#   FIXED: rows flush every batch_interval_ms throughout the stream
query = (
    stream_df.writeStream.foreach(writer)
    .outputMode("update")
    .trigger(realTime="30 seconds")
    .option("checkpointLocation", f"/tmp/rtm_latency_ckpt/{VARIANT.lower()}")
    .start()
)

print(f"Streaming query started: {query.id}")
print(f"Will run for {DURATION_MINUTES} minute(s)...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run for Duration, Then Stop

# COMMAND ----------

import time

run_seconds = DURATION_MINUTES * 60
start = time.time()

# Poll query status while running
while time.time() - start < run_seconds:
    if not query.isActive:
        print(f"Query stopped unexpectedly!")
        if query.exception():
            print(f"Exception: {query.exception()}")
        break
    elapsed = time.time() - start
    progress = query.lastProgress
    if progress:
        num_rows = progress.get("numInputRows", 0)
        print(f"  [{elapsed:.0f}s] Active, {num_rows} rows in last batch")
    time.sleep(10)

# Stop the RTM consumer query
query.stop()
print(f"Query stopped after {time.time() - start:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results: Query Lakebase

# COMMAND ----------

with psycopg.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        # Summary
        cur.execute(
            f"""
            SELECT variant,
                   COUNT(*) AS total_rows,
                   COUNT(DISTINCT date_trunc('second', sink_ts)) AS distinct_sink_seconds,
                   MIN(sink_ts) AS first_sink,
                   MAX(sink_ts) AS last_sink,
                   MAX(sink_ts) - MIN(sink_ts) AS sink_span,
                   AVG(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts))) AS avg_latency_sec,
                   MAX(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts))) AS max_latency_sec,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY EXTRACT(EPOCH FROM (sink_ts - pipeline_ts))
                   ) AS p50_latency_sec,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (
                       ORDER BY EXTRACT(EPOCH FROM (sink_ts - pipeline_ts))
                   ) AS p99_latency_sec
            FROM {TABLE_NAME}
            WHERE variant = %s
            GROUP BY 1
        """,
            (VARIANT,),
        )
        row = cur.fetchone()
        if row:
            print(f"\n{'=' * 60}")
            print(f" {VARIANT} WRITER RESULTS")
            print(f"{'=' * 60}")
            print(f"  Total rows:            {row[1]:,}")
            print(f"  Distinct sink seconds: {row[2]}")
            print(f"  Sink span:             {row[5]}")
            print(f"  Avg latency:           {row[6]:.2f}s")
            print(f"  P50 latency:           {row[8]:.2f}s")
            print(f"  P99 latency:           {row[9]:.2f}s")
            print(f"  Max latency:           {row[7]:.2f}s")
        else:
            print(f"No rows found for variant={VARIANT}")

        # Per-second sink pattern (first 60 seconds)
        print(f"\n--- Per-second sink counts (first 60s) ---")
        cur.execute(
            f"""
            SELECT date_trunc('second', sink_ts) AS sec,
                   COUNT(*) AS rows_arrived
            FROM {TABLE_NAME}
            WHERE variant = %s
            GROUP BY 1
            ORDER BY 1
            LIMIT 60
        """,
            (VARIANT,),
        )
        for r in cur.fetchall():
            bar = "#" * min(int(r[1] / 100), 50)
            print(f"  {r[0]} | {r[1]:>6} rows | {bar}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison Query (run after BOTH jobs complete)
# MAGIC
# MAGIC ```sql
# MAGIC -- Summary comparison
# MAGIC SELECT variant,
# MAGIC        COUNT(*) AS total_rows,
# MAGIC        COUNT(DISTINCT date_trunc('second', sink_ts)) AS distinct_sink_seconds,
# MAGIC        ROUND(AVG(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 2) AS avg_latency_sec,
# MAGIC        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
# MAGIC            ORDER BY EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 2) AS p50_latency_sec,
# MAGIC        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (
# MAGIC            ORDER BY EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 2) AS p99_latency_sec,
# MAGIC        ROUND(MAX(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 2) AS max_latency_sec
# MAGIC FROM rtm_latency_test
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;
# MAGIC
# MAGIC -- Per-second arrival pattern (shows gaps for BUGGY, steady for FIXED)
# MAGIC SELECT variant,
# MAGIC        date_trunc('second', sink_ts) AS arrival_sec,
# MAGIC        COUNT(*) AS rows_arrived,
# MAGIC        ROUND(AVG(EXTRACT(EPOCH FROM (sink_ts - pipeline_ts)))::numeric, 2) AS avg_latency
# MAGIC FROM rtm_latency_test
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2;
# MAGIC ```
