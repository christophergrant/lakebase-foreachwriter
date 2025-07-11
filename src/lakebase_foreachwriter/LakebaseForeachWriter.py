# lakebase_writer.py

import contextlib
import logging
import threading
import time
from typing import List, Optional, Sequence, Tuple

import psycopg
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import ArrayType, DataType, MapType, StructType


def _build_conn_params(lakebase_name: str, user: str, password: str, read_write_dns: str) -> dict:
    """
    Constructs the connection parameters for a Lakebase database.

    If the read_write_dns is not provided, it will be dynamically retrieved
    using the Databricks SDK.

    Args:
        lakebase_name: The name of the Lakebase instance.
        user: The username for authentication.
        password: The password or token for authentication.
        read_write_dns: The DNS name for the read-write endpoint of the Lakebase.

    Returns:
        A dictionary of connection parameters suitable for psycopg.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    if not read_write_dns:
        ws = WorkspaceClient(config=Config())
        read_write_dns  = ws.database.get_database_instance(lakebase_name).read_write_dns
    return {
        "host": read_write_dns,
        "port": 5432,
        "dbname": "databricks_postgres",
        "user": user,
        "password": password,
        "sslmode": "require",
    }


# ────────────────────────── MAIN FOREACH WRITER ──────────────────────────
class LakebaseForeachWriter:
    """
    A PySpark Structured Streaming ForeachWriter for writing data to a Lakebase
    database.

    This writer is designed to be used with `df.writeStream.foreach()`. It handles
    database connections on each Spark executor, and supports batching of records
    for efficient writing. It offers three writing modes: 'insert', 'upsert', and
    'bulk-insert' (using PostgreSQL's COPY command).

    The writer is serializable and can be sent from the driver to the executors.
    Runtime objects that are not serializable, such as database connections and
    loggers, are initialized within the `open` method on each executor.

    Attributes:
        lakebase_name (str): The name of the target Lakebase.
        conn_params (dict): Connection parameters for the database.
        table (str): The target database table.
        mode (str): The write mode: "insert", "upsert", or "bulk-insert".
        primary_keys (List[str]): A list of primary key columns, required for 'upsert' mode.
        batch_size (int): The number of rows to buffer before flushing.
        batch_interval_ms (int): The maximum time in milliseconds to wait before flushing.
        columns (List[str]): The columns of the DataFrame being written.
        insert_sql (Optional[str]): The SQL statement for 'insert' or 'upsert' operations.
    """

    # ------------------------------------------------------------------ #
    # ctor → runs only on the driver
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        username: str,
        password: str,
        lakebase_name: str,
        table: str,
        df: DataFrame,
        mode: str = "insert",  # "insert" | "upsert" | "bulk-insert"
        primary_keys: Optional[Sequence[str]] = None,
        read_write_dns: str = None,
        batch_size: int = 1_000,
        batch_interval_ms: int = 100,
    ):
        """
        Initializes the LakebaseForeachWriter on the Spark driver.

        Args:
            username: The username for the Lakebase connection.
            password: The password (or token) for the Lakebase connection.
            lakebase_name: The name of the target Lakebase instance.
            table: The name of the target table to write to.
            df: The DataFrame being written. This is used to infer the schema.
            mode: The write mode. Can be one of "insert", "upsert", or "bulk-insert".
                  Defaults to "insert".
            primary_keys: A sequence of column names that constitute the primary key.
                          Required for 'upsert' mode. Defaults to None.
            read_write_dns: The read-write DNS of the Lakebase. If not provided, it's
                            retrieved using the Databricks SDK and lakebase_name. Defaults to None.
            batch_size: The number of rows to collect in a buffer before writing to the
                        database. Defaults to 1_000.
            batch_interval_ms: The time in milliseconds to wait before forcing a flush
                               of the buffer, even if it's not full. Defaults to 100.
        """
        # ----- schema validation --------------------------------------------------
        offending_fields = self._find_complex_type_offenders(df.schema)
        if offending_fields:
            raise ValueError(
                "DataFrame schema contains unsupported complex types (StructType, MapType) "
                "that do not map cleanly to standard PostgreSQL columns. "
                f"Offending fields: {sorted(offending_fields)}"
            )

        # ----- serialisable config ------------------------------------------------
        self.lakebase_name = lakebase_name
        self.conn_params = _build_conn_params(lakebase_name, username, password, read_write_dns)
        self.table = table
        self.mode = mode.lower()
        self.primary_keys = list(primary_keys or [])
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms
        self.columns: List[str] = df.schema.names or (
            (_ for _ in ()).throw(ValueError("DataFrame has no columns"))
        )
        self.insert_sql = self._build_insert_sql()

        # ----- driver-only runtime objects ----------------------------------------
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self._stop_event = threading.Event()

        # executor-side handles (created in open())
        self.conn = self.cur = None
        self.row_buffer: List[tuple] = []
        self.last_flush_time: Optional[float] = None

    # ------------------------------------------------------------------ #
    # custom pickling → keep writer serialisable
    # ------------------------------------------------------------------ #
    def __getstate__(self):
        """
        Prepares the writer object for serialization to be sent to executors.

        Removes non-serializable attributes like loggers, connection objects,
        and threading primitives.
        """
        state = self.__dict__.copy()
        for k in ("_stop_event", "logger", "conn", "cur"):
            state.pop(k, None)
        return state

    def __setstate__(self, state):
        """
        Reinitializes the writer object on the executor after deserialization.

        Sets up a logger and initializes fields for connection and buffer state.
        """
        self.__dict__.update(state)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self._stop_event = None
        self.conn = self.cur = None
        self.row_buffer = []
        self.last_flush_time = None

    # ------------------------------------------------------------------ #
    # ForeachWriter lifecycle
    # ------------------------------------------------------------------ #
    def open(self, partition_id: int, epoch_id: int) -> bool:
        """
        Initializes the connection on the executor.

        This method is called once per partition and epoch before processing any
        rows. It establishes a connection to the Lakebase database.

        Args:
            partition_id: The ID of the partition being processed.
            epoch_id: The ID of the streaming epoch.

        Returns:
            True if the connection was successful, False otherwise.
        """
        try:
            self.conn = psycopg.connect(
                **self.conn_params, autocommit=True
            )
            self.cur = self.conn.cursor()
            self.last_flush_time = time.time() * 1000
            self.logger.info(f"Opened DB connection p{partition_id} e{epoch_id}")
            return True
        except Exception as exc:
            self.logger.error(f"open() failed: {exc}", exc_info=True)
            return False

    def process(self, row: Row) -> None:
        """
        Processes a single row of data on the executor.

        The row is added to an in-memory buffer. The buffer is flushed to the
        database if it reaches the configured `batch_size` or if the
        `batch_interval_ms` has elapsed since the last flush.

        Args:
            row: The Row object to process.
        """
        self.row_buffer.append(tuple(row[c] for c in self.columns))
        now_ms = time.time() * 1000
        if (
            len(self.row_buffer) >= self.batch_size
            or now_ms - self.last_flush_time >= self.batch_interval_ms
        ):
            self._flush_buffer()
            self.last_flush_time = now_ms

    def close(self, error) -> None:
        """
        Cleans up resources on the executor when the writer is closing.

        This method is called after all rows in a partition have been processed.
        It ensures any remaining rows in the buffer are flushed and closes the
        database connection.

        Args:
            error: The error that caused the writer to close, or None if it closed cleanly.
        """
        try:
            if self.row_buffer:
                self._flush_buffer()
        finally:
            with contextlib.suppress(Exception):
                if self.cur:
                    self.cur.close()
                if self.conn:
                    self.conn.close()
            # driver: stop refresher
            if self._stop_event:
                self._stop_event.set()
            self.logger.info(
                "Writer closed" + (f" with error {error}" if error else "")
            )

    # ------------------------------------------------------------------ #
    # internal helpers
    # ------------------------------------------------------------------ #
    def _find_complex_type_offenders(self, schema: StructType) -> List[str]:
        """
        Traverses the DataFrame schema to find fields with unsupported complex
        types (StructType, MapType) that do not map cleanly to standard
        database columns.

        Args:
            schema: The StructType schema to check.

        Returns:
            A list of string paths to the offending fields.
        """
        offenders: List[str] = []
        # Use a stack for depth-first traversal. Each item is (DataType, path_string).
        stack: List[Tuple[DataType, str]] = [
            (field.dataType, field.name) for field in schema.fields
        ]

        while stack:
            current_type, current_path = stack.pop()

            # Offending types are StructType and MapType, as they don't have a
            # direct, flat representation in a standard PG column.
            if isinstance(current_type, (StructType, MapType)):
                offenders.append(current_path)

            # --- Traverse into nested structures ---
            # If it's a struct, check its children.
            if isinstance(current_type, StructType):
                for field in current_type.fields:
                    stack.append((field.dataType, f"{current_path}.{field.name}"))
            # If it's an array, check its element type.
            elif isinstance(current_type, ArrayType):
                stack.append((current_type.elementType, f"{current_path}[]"))

        return offenders

    def _build_insert_sql(self) -> Optional[str]:
        """
        Constructs the SQL statement for 'insert' or 'upsert' operations.

        Returns:
            The generated SQL string, or None for 'bulk-insert' mode.

        Raises:
            ValueError: If `mode` is 'upsert' and `primary_keys` is not set,
                        or if `mode` is invalid.
        """
        placeholders = ", ".join(["%s"] * len(self.columns))
        cols_joined = ", ".join(self.columns)

        if self.mode == "insert":
            return f"INSERT INTO {self.table} ({cols_joined}) VALUES ({placeholders})"

        if self.mode == "upsert":
            if not self.primary_keys:
                raise ValueError("primary_keys required for upsert mode")
            non_pk = [c for c in self.columns if c not in self.primary_keys]
            set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in non_pk)
            pk_joined = ", ".join(self.primary_keys)
            return (
                f"INSERT INTO {self.table} ({cols_joined}) VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_joined}) DO UPDATE SET {set_clause}"
            )

        if self.mode == "bulk-insert":
            return None  # COPY handled in _flush_buffer

        raise ValueError('mode must be "insert", "upsert", or "bulk-insert"')

    def _flush_buffer(self) -> None:
        """
        Writes the buffered rows to the database.

        This method handles the actual database write operation based on the
        configured `mode`. It uses `executemany` for 'insert' and 'upsert',
        and the `COPY` command for 'bulk-insert'.
        """
        if not self.row_buffer:
            return
        try:
            if self.mode == "bulk-insert":
                cols = ", ".join(self.columns)
                copy_sql = f"COPY {self.table} ({cols}) FROM STDIN"
                with self.conn.cursor() as cur:
                    with cur.copy(copy_sql) as cpy:
                        for vals in self.row_buffer:
                            cpy.write_row(vals)
            else:
                self.cur.executemany(self.insert_sql, self.row_buffer)
            self.logger.info(f"Flushed {len(self.row_buffer)} rows ({self.mode})")
        except Exception as exc:
            self.logger.error(
                f"Flush failed: {exc}; reconnecting & retrying", exc_info=True
            )
            self._retry_flush()
        finally:
            self.row_buffer = []

    def _retry_flush(self) -> None:
        """
        Attempts to reconnect and retry a failed flush operation once.

        If the initial flush fails, this method closes the existing connection,
        establishes a new one, and retries the write operation. If the retry
        also fails, it raises an exception to fail the Spark task.
        """
        with contextlib.suppress(Exception):
            if self.conn:
                self.conn.close()

        self.conn = psycopg.connect(
            **self.conn_params, autocommit=True
        )
        self.cur = self.conn.cursor()

        try:
            if self.mode == "bulk-insert":
                cols = ", ".join(self.columns)
                copy_sql = f"COPY {self.table} ({cols}) FROM STDIN"
                with self.conn.cursor() as cur:
                    with cur.copy(copy_sql) as cpy:
                        for vals in self.row_buffer:
                            cpy.write_row(vals)
            else:
                self.cur.executemany(self.insert_sql, self.row_buffer)
            self.logger.info("Retry succeeded.")
        except Exception as exc2:
            self.logger.error(
                f"Retry failed for {len(self.row_buffer)} rows. Raising exception.",
                exc_info=True,
            )
            self.row_buffer = []
            raise exc2