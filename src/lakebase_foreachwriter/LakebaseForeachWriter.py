import logging
import queue
import sys
import threading
import time
from collections.abc import Callable, Sequence

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import ArrayType, MapType, StructType


logger = logging.getLogger("lakebase_foreachwriter")

_host_cache: dict[str, str] = {}


def _resolve_host(ws: WorkspaceClient, lakebase_name: str) -> str:
    """Resolve a Lakebase instance name to a host, supporting both Provisioned and Autoscaling.

    Results are cached for the lifetime of the process so that subsequent
    partitions and micro-batches skip the API round-trip entirely.
    """
    if lakebase_name in _host_cache:
        return _host_cache[lakebase_name]

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _try_provisioned():
        return ws.database.get_database_instance(lakebase_name).read_write_dns

    def _try_autoscaling():
        endpoints = list(
            ws.postgres.list_endpoints(
                parent=f"projects/{lakebase_name}/branches/production"
            )
        )
        for ep in endpoints:
            if ep.status and "READ_WRITE" in str(ep.status.endpoint_type):
                return ep.status.hosts.host
        return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(_try_provisioned): "provisioned",
            pool.submit(_try_autoscaling): "autoscaling",
        }
        for future in as_completed(futures):
            try:
                host = future.result()
                if host:
                    _host_cache[lakebase_name] = host
                    return host
            except Exception:
                continue

    raise ValueError(
        f"No Lakebase instance found for '{lakebase_name}'. "
        "Checked both Provisioned (database instance) and Autoscaling (postgres project)."
    )


def _build_conn_params(
    user: str,
    password: str,
    lakebase_name: str | None = None,
    host: str | None = None,
    sslmode: str | None = None,
) -> dict:
    """Build connection parameters for lakebase database."""
    if not host:
        if not lakebase_name:
            raise ValueError("Either host or lakebase_name must be provided")
        ws = WorkspaceClient(config=Config())
        host = _resolve_host(ws, lakebase_name)

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
        "sslmode": sslmode or "require",
    }


def oauth_credential_provider(
    workspace_client: WorkspaceClient | None = None,
) -> Callable[[], tuple[str, str]]:
    """Create a credential provider that generates fresh OAuth tokens from the Databricks SDK.

    Returns a callable that produces (username, password) on each invocation,
    with token caching to avoid unnecessary round-trips.

    Usage:
        writer = LakebaseForeachWriter(
            credential_provider=oauth_credential_provider(),
            table="my_table",
            df=streaming_df,
            lakebase_name="my_lakebase",
        )
    """
    _lock = threading.Lock()
    _cached_token: str | None = None
    _cached_user: str | None = None
    _expires_at: float = 0

    def _provide() -> tuple[str, str]:
        nonlocal _cached_token, _cached_user, _expires_at

        with _lock:
            now = time.time()
            if _cached_token and _cached_user and now < _expires_at:
                return _cached_user, _cached_token

            ws = workspace_client or WorkspaceClient(config=Config())

            me = ws.current_user.me()
            _cached_user = me.user_name or me.application_id

            cred = ws.postgres.generate_database_credential()
            _cached_token = cred.password
            # Refresh 5 minutes before the 60-minute expiry
            _expires_at = now + 55 * 60

            logger.info("Refreshed OAuth database credential")
            return _cached_user, _cached_token

    return _provide


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
        table: str,
        df: DataFrame,
        username: str | None = None,
        password: str | None = None,
        credential_provider: Callable[[], tuple[str, str]] | None = None,
        lakebase_name: str | None = None,
        host: str | None = None,
        sslmode: str | None = None,
        mode: str = "insert",
        primary_keys: Sequence[str] | None = None,
        batch_size: int = 1000,
        batch_interval_ms: int = 100,
        max_queue_size: int = 10_000,
        max_retries: int = 3,
        retry_base_delay_s: float = 0.5,
    ):
        if not credential_provider and not (username and password):
            raise ValueError(
                "Either credential_provider or both username and password must be provided"
            )

        # validate unsupported types
        unsupported = self._find_unsupported_fields(df.schema)
        if unsupported:
            raise ValueError(
                f"Unsupported field types found: {', '.join(unsupported)}. "
                "Please convert complex types to supported formats first."
            )

        self.credential_provider = credential_provider
        self.username = username or ""
        self.password = password or ""
        self.table = table
        self.mode = mode.lower()
        self.columns = df.schema.names
        self.primary_keys = primary_keys if primary_keys else []
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms
        self.max_queue_size = max_queue_size
        self.max_retries = max_retries
        self.retry_base_delay_s = retry_base_delay_s

        # Resolve initial credentials
        if self.credential_provider:
            self.username, self.password = self.credential_provider()

        # Initialize connection parameters
        self.conn_params = _build_conn_params(
            user=self.username,
            password=self.password,
            lakebase_name=lakebase_name,
            host=host,
            sslmode=sslmode,
        )

        # Local development convenience: if connecting to a local Postgres,
        # default to disabling SSL and using the default 'postgres' database
        # unless explicitly configured via lakebase_name.
        if host and host in {"localhost", "127.0.0.1"} and not lakebase_name:
            try:
                # Prefer caller-provided sslmode if given
                self.conn_params["sslmode"] = sslmode or "disable"
                # Default dbname to the standard local database
                self.conn_params["dbname"] = "postgres"
            except Exception:
                # Best-effort adjustment; leave as-is on any issue
                pass

        # Initialize runtime state
        self.conn: psycopg.Connection | None = None
        self.cur: psycopg.Cursor | None = None
        self.queue: queue.Queue | None = None
        self.stop_event: threading.Event | None = None
        self.worker_thread: threading.Thread | None = None
        self.current_batch: list = []
        self.last_flush = time.time()
        self.total_rows_processed = 0
        self.total_batches_flushed = 0
        self.sql = self._build_sql()

    def _refresh_credentials(self):
        """Refresh credentials from provider and update conn_params."""
        if self.credential_provider:
            self.username, self.password = self.credential_provider()
            self.conn_params["user"] = self.username
            self.conn_params["password"] = self.password

    def open(self, partition_id: int, epoch_id: int) -> bool:
        """Open database connection and start background worker."""
        try:
            self.partition_id = partition_id
            self.epoch_id = epoch_id

            # Ensure the module logger can emit on this executor.
            # Each executor is a separate Python process whose logger
            # starts with no handlers, so INFO messages are silently
            # dropped.  Adding a stderr handler here (idempotently)
            # makes flush/retry/close messages visible in the executor
            # logs without touching the root logger.
            if not logger.handlers:
                handler = logging.StreamHandler(sys.stderr)
                handler.setFormatter(
                    logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
                )
                logger.addHandler(handler)
                logger.propagate = False
            logger.setLevel(logging.INFO)

            # Refresh credentials before connecting (handles OAuth token expiry)
            self._refresh_credentials()

            # Connect to database
            self.conn = psycopg.connect(**self.conn_params)

            # Initialize threading
            self.queue = queue.Queue(maxsize=self.max_queue_size)
            self.stop_event = threading.Event()
            self.batch = []
            self.last_flush = time.time()
            self.worker_error = None

            # Start background worker
            self.worker_thread = threading.Thread(target=self._worker, daemon=True)
            self.worker_thread.start()

            logger.info(
                f"[{partition_id}|{epoch_id}] Opening writer for table {self.table} with schema {self.columns}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to open writer: {e}")
            return False

    def process(self, row: Row | tuple):
        if self.worker_error:
            raise Exception(f"Worker failed: {self.worker_error}")

        if isinstance(row, Row):
            row_data = tuple(row[col] for col in self.columns)
        else:
            row_data = tuple(row)

        while True:
            if self.worker_error:
                raise Exception(f"Worker failed: {self.worker_error}")
            try:
                self.queue.put(row_data, timeout=1.0)
                return
            except queue.Full:
                continue

    def close(self, error: Exception | None):
        """Close writer and flush remaining data."""
        try:
            if self.stop_event:
                self.stop_event.set()

            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=30.0)
                if self.worker_thread.is_alive():
                    logger.error(
                        f"[{self.partition_id}|{self.epoch_id}] Worker thread did not stop within 30s"
                    )

            if self.queue and self.queue.qsize() > self.batch_size * 5:
                logger.warning(
                    f"[{self.partition_id}|{self.epoch_id}] \
                While closing the writer, remaining queue size is {self.queue.qsize()}, which is more than 5x the batch size. \
                This may be indicative of an overwhelmed or misconfigured writer."
                )

            if self.queue:
                self._flush_remaining()

        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass

        logger.info(f"[{self.partition_id}|{self.epoch_id}] Writer closed")

    def _worker(self):
        """Background worker that processes queue and flushes batches."""
        while not self.stop_event.is_set():
            try:
                # de-queue items into batch list
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
                    self._flush_with_retry()

                time.sleep(0.0001)  # 100us sleep

            except Exception as e:
                logger.error(f"Worker error: {e}")
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
            logger.info(
                f"[{self.partition_id}|{self.epoch_id}] Flushed {batch_size} rows in {perf_time:.1f}ms"
            )

        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise

    def _reconnect(self):
        """Close existing connection and re-establish it."""
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        try:
            self._refresh_credentials()
            self.conn = psycopg.connect(**self.conn_params)
            logger.info(
                f"[{self.partition_id}|{self.epoch_id}] Reconnected to database"
            )
        except Exception as e:
            logger.error(f"[{self.partition_id}|{self.epoch_id}] Reconnect failed: {e}")
            raise

    def _flush_with_retry(self):
        """Flush batch with exponential backoff retry on failure."""
        last_exception: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                self._flush_batch()
                return
            except Exception as e:
                last_exception = e
                if attempt == self.max_retries:
                    raise
                delay = self.retry_base_delay_s * (2**attempt)
                logger.warning(
                    f"[{self.partition_id}|{self.epoch_id}] Flush attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                # Interruptible sleep in 100ms increments
                elapsed = 0.0
                while elapsed < delay:
                    if self.stop_event and self.stop_event.is_set():
                        break
                    time.sleep(min(0.1, delay - elapsed))
                    elapsed += 0.1
                try:
                    self._reconnect()
                except Exception:
                    pass  # Next flush attempt will catch it
        if last_exception is None:
            raise RuntimeError("flush retry exited without an exception")
        raise last_exception

    def _flush_remaining(self):
        """Flush any remaining items during shutdown."""
        while True:
            try:
                self.batch.append(self.queue.get_nowait())
            except queue.Empty:
                break

        if self.batch:
            self._flush_with_retry()

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
