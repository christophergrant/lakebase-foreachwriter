import logging
import queue
import threading
import time
from collections.abc import Sequence

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import ArrayType, MapType, StructType


def _build_conn_params(
    user: str, password: str, lakebase_name: str | None = None, host: str | None = None
) -> dict:
    """Build connection parameters for lakebase database."""
    if not host:
        if not lakebase_name:
            raise ValueError("Either host or lakebase_name must be provided")
        ws = WorkspaceClient(config=Config())
        host = ws.database.get_database_instance(lakebase_name).read_write_dns

    if not host:
        raise ValueError(
            "host is required but was not provided and could not be retrieved."
        )

    return {
        "host": host,
        "port": 5432,
        "dbname": "databricks_postgres",
        "user": user,
        "password": password,
        "sslmode": "require",
    }


class LakebaseForeachWriter:
    """
    Writes streaming Spark DataFrames to a Lakebase database efficiently.

    This writer will work with all Lakebase use cases,
        but was built for use with Spark's Real-time mode.

    Quick Start Example:
    ```python
    # Create the writer
    writer = LakebaseForeachWriter(
        username="your_username",
        password="your_token",
        table="my_table",
        df=your_dataframe,
        lakebase_name="my_lakebase"
    )

    # Use it in your streaming query
    query = (your_streaming_df
             .writeStream
             .foreach(writer)
             .trigger(processingTime='10 seconds')
             .start())
    ```

    Tuning to your use case:
    - For low-latency applications, set batch_interval_ms to something small like 10ms.
    - For high-throughput applications, set batch_size to something large like 10000, set batch_interval_ms to something large like 1000ms, and mode to "bulk-insert" if applicable.

    Write modes:
    - "insert": Appends new rows via execute_many
    - "upsert": Updates existing rows or inserts new ones via execute_many (requires primary_keys)
    - "bulk-insert": Uses COPY to efficiently insert large batches of data
    """

    def __init__(
        self,
        username: str,
        password: str,
        table: str,
        df: DataFrame,
        lakebase_name: str | None = None,
        host: str | None = None,
        mode: str = "insert",
        primary_keys: Sequence[str] | None = None,
        batch_size: int = 1000,
        batch_interval_ms: int = 100,
    ):
        # validate unsupported types
        unsupported = self._find_unsupported_fields(df.schema)
        if unsupported:
            raise ValueError(
                f"Unsupported field types found: {', '.join(unsupported)}. "
                "Please convert complex types to supported formats first."
            )

        self.username = username
        self.password = password
        self.table = table
        self.mode = mode.lower()
        self.columns = df.schema.names
        self.primary_keys = primary_keys if primary_keys else []
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms

        # Initialize connection parameters
        self.conn_params = _build_conn_params(
            user=username,
            password=password,
            lakebase_name=lakebase_name,
            host=host,
        )

        # Initialize runtime state
        self.conn: psycopg.Connection | None = None
        self.cur: psycopg.Cursor | None = None
        self.queue: queue.SimpleQueue | None = None
        self.stop_event: threading.Event | None = None
        self.worker_thread: threading.Thread | None = None
        self.current_batch: list = []
        self.last_flush = time.time()
        self.total_rows_processed = 0
        self.total_batches_flushed = 0
        self.sql = self._build_sql()

    def open(self, partition_id: int, epoch_id: int) -> bool:
        """Open database connection and start background worker."""
        try:
            self.partition_id = partition_id
            self.epoch_id = epoch_id

            # Connect to database
            self.conn = psycopg.connect(**self.conn_params)

            # Initialize threading
            self.queue = queue.SimpleQueue()
            self.stop_event = threading.Event()
            self.batch = []
            self.last_flush = time.time()
            self.worker_error = None

            # Start background worker
            self.worker_thread = threading.Thread(target=self._worker, daemon=True)
            self.worker_thread.start()

            logging.info(
                f"[{partition_id}|{epoch_id}] Opening writer for table {self.table} with schema {self.columns}"
            )
            return True

        except Exception as e:
            logging.error(f"Failed to open writer: {e}")
            return False

    def process(self, row: Row):
        """Add row to processing queue."""
        if self.worker_error:
            raise Exception(f"Worker failed: {self.worker_error}")

        row_data = tuple(row[col] for col in self.columns)
        self.queue.put(row_data)

    def close(self, error: Exception | None):
        """Close writer and flush remaining data."""
        try:
            if self.stop_event:
                self.stop_event.set()

            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=5.0)

            if self.queue.qsize() > self.batch_size * 5:
                logging.warning(
                    f"[{self.partition_id}|{self.epoch_id}] \
                While closing the writer, remaining queue size is {self.queue.qsize()}, which is more than 5x the batch size. \
                This may be indicative of an overwhelmed or misconfigured writer."
                )

            self._flush_remaining()

        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass

        logging.info(f"[{self.partition_id}|{self.epoch_id}] Writer closed")

    def _worker(self):
        """Background worker that processes queue and flushes batches.
        Flush timing is based off of queue size or time threshold.
        """
        while not self.stop_event.is_set():
            try:
                # de-queue items into batch list
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

                time.sleep(0.0001)  # 100us sleep

            except Exception as e:
                logging.error(f"Worker error: {e}")
                self.worker_error = str(e)
                break

    def _flush_batch(self):
        """Flush current batch to database.
        The last log message represents the total time to flush the batch via _flush_remaining.
        """
        if not self.batch:
            return
        perf_start = None
        try:
            with self.conn.cursor() as cur:
                if self.mode == "bulk-insert":
                    cols = ", ".join(self.columns)
                    perf_start = time.time()
                    with cur.copy(f"COPY {self.table} ({cols}) FROM STDIN") as copy:
                        for row in self.batch:
                            copy.write_row(row)
                else:
                    perf_start = time.time()
                    cur.executemany(self.sql, self.batch)

            self.conn.commit()
            batch_size = len(self.batch)
            self.batch = []
            self.last_flush = time.time()

            perf_time = (time.time() - perf_start) * 1000
            logging.info(
                f"[{self.partition_id}|{self.epoch_id}] Flushed {batch_size} rows in {perf_time:.1f}ms"
            )

        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise

    def _flush_remaining(self):
        """Flush any remaining items during shutdown."""
        while True:
            try:
                self.batch.append(self.queue.get_nowait())
            except queue.Empty:
                break

        if self.batch:
            self._flush_batch()

    def _time_to_flush(self) -> bool:
        """Check if enough time has passed to trigger flush."""
        return (time.time() - self.last_flush) * 1000 >= self.batch_interval_ms

    def _build_sql(self) -> str | None:
        """Build SQL statement based on mode."""
        cols = ", ".join(self.columns)
        placeholders = ", ".join(["%s"] * len(self.columns))

        if self.mode == "insert":
            return f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"

        elif self.mode == "upsert":
            if not self.primary_keys:
                raise ValueError("primary_keys required for upsert mode")
            pk_cols = ", ".join(self.primary_keys)
            update_cols = ", ".join(
                [
                    f"{c} = EXCLUDED.{c}"
                    for c in self.columns
                    if c not in self.primary_keys
                ]
            )
            return f"""
                INSERT INTO {self.table} ({cols}) VALUES ({placeholders})
                ON CONFLICT ({pk_cols}) DO UPDATE SET {update_cols}
            """

        elif self.mode == "bulk-insert":
            return None  # bulk-insert is straightforward and done entirely in _flush_batch()

        else:
            raise ValueError(f"Invalid mode: {self.mode}")

    @staticmethod
    def _find_unsupported_fields(schema: StructType) -> list[str]:
        """Find fields with unsupported data types."""
        unsupported = []
        unsupported_types = (StructType, MapType)

        for field in schema.fields:
            if isinstance(field.dataType, unsupported_types):
                unsupported.append(field.name)
            elif isinstance(field.dataType, ArrayType) and isinstance(
                field.dataType.elementType, unsupported_types
            ):
                unsupported.append(field.name)

        return unsupported
