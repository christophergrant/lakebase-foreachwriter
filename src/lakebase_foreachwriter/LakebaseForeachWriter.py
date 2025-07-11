# lakebase_writer.py

import contextlib
import logging
import threading
import time
from typing import List, Optional, Sequence, Any

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import ArrayType, MapType, StructType


def _build_conn_params(
    user: str,
    password: str,
    lakebase_name: Optional[str] = None,
    read_write_dns: Optional[str] = None,
) -> dict:
    """
    Constructs the connection parameters for a Lakebase database.

    Args:
        user: The username for authentication.
        password: The password or token for authentication.
        lakebase_name: The name of the Lakebase instance. Required if read_write_dns is not provided.
        read_write_dns: The DNS name for the read-write endpoint of the Lakebase.

    Returns:
        A dictionary of connection parameters for psycopg.
    """
    if not read_write_dns:
        if not lakebase_name:
            # This should be caught by the writer's __init__, but is here as a safeguard.
            raise ValueError("Cannot look up read_write_dns without a lakebase_name.")
        ws = WorkspaceClient(config=Config())
        read_write_dns = ws.database.get_database_instance(lakebase_name).read_write_dns

    if not read_write_dns:
        raise ValueError(
            "read_write_dns is required but was not provided and could not be retrieved."
        )

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
    A PySpark ForeachWriter for writing a streaming DataFrame to a Lakebase database.
    """

    def __init__(
        self,
        username: str,
        password: str,
        table: str,
        df: DataFrame,
        lakebase_name: Optional[str] = None,
        mode: str = "insert",  # "insert" | "upsert" | "bulk-insert"
        primary_keys: Optional[Sequence[str]] = None,
        read_write_dns: Optional[str] = None,
        batch_size: int = 1_000,
        batch_interval_ms: int = 100,
    ):
        """
        Initializes the LakebaseForeachWriter on the Spark driver.

        Args:
            username: The username for the Lakebase connection.
            password: The password (or token) for the Lakebase connection.
            table: The name of the target table to write to.
            df: The DataFrame being written. This is used to infer the schema.
            lakebase_name: The name of the target Lakebase instance. Required if
                          `read_write_dns` is not provided. Defaults to None.
            mode: The write mode. Can be one of "insert", "upsert", or "bulk-insert".
                  Defaults to "insert".
            primary_keys: A sequence of column names that constitute the primary key.
                          Required for 'upsert' mode. Defaults to None.
            read_write_dns: The read-write DNS of the Lakebase. If not provided, it's
                            retrieved using the Databricks SDK and `lakebase_name`. Defaults to None.
            batch_size: The number of rows to collect in a buffer before writing to the
                        database. Defaults to 1_000.
            batch_interval_ms: The time in milliseconds to wait before forcing a flush
                               of the buffer, even if it's not full. Defaults to 100.
        """
        if not lakebase_name and not read_write_dns:
            raise ValueError(
                "Either `lakebase_name` or `read_write_dns` must be provided."
            )

        offending_fields = self._find_complex_type_offenders(df.schema)
        if offending_fields:
            raise ValueError(
                f"Unsupported complex types found in schema: {', '.join(offending_fields)}"
            )

        self.lakebase_name = lakebase_name
        self.conn_params = _build_conn_params(
            user=username,
            password=password,
            lakebase_name=lakebase_name,
            read_write_dns=read_write_dns,
        )
        self.table = table
        self.mode = mode.lower()
        self.columns = df.schema.names
        self.primary_keys = primary_keys if primary_keys else []

        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms
        self.insert_sql = self._build_insert_sql()

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
        try:
            self.conn = psycopg.connect(**self.conn_params, autocommit=True)
            self.cur = self.conn.cursor()
            self.row_buffer = []
            self.last_flush_time = time.time()
            # This logging is helpful for debugging stream progress.
            logging.info(f"Opened DB connection p{partition_id} e{epoch_id}")
            return True
        except Exception as e:
            logging.error(f"open() failed: {e}", exc_info=True)
            return False

    def process(self, row: Row):
        self.row_buffer.append(tuple(row[c] for c in self.columns))

        if len(self.row_buffer) >= self.batch_size or self._time_to_flush():
            self._flush_buffer()

    def close(self, error: Optional[Exception]):
        if hasattr(self, "conn") and self.conn:
            try:
                if self.row_buffer:
                    self._flush_buffer()
            except Exception as e:
                logging.error(f"Error during final flush in close(): {e}")

            if self.cur:
                self.cur.close()
            self.conn.close()

    # ------------------------------------------------------------------ #
    # internal helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _find_complex_type_offenders(schema: StructType) -> List[str]:
        offenders = []
        for field in schema.fields:
            if isinstance(field.dataType, (StructType, MapType)):
                offenders.append(field.name)
        return offenders

    def _build_insert_sql(self) -> Optional[str]:
        """
        Constructs the SQL statement for 'insert' or 'upsert' operations.
        For 'bulk-insert', the COPY command is handled directly in _flush_buffer.
        """
        cols = ", ".join(self.columns)
        placeholders = ", ".join(["%s"] * len(self.columns))

        if self.mode == "insert":
            return f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"

        if self.mode == "upsert":
            if not self.primary_keys:
                raise ValueError("primary_keys required for upsert mode")
            pk_cols = ", ".join(self.primary_keys)
            update_cols = ", ".join(
                [f"{c} = EXCLUDED.{c}" for c in self.columns if c not in self.primary_keys]
            )
            return f"""
                INSERT INTO {self.table} ({cols}) VALUES ({placeholders})
                ON CONFLICT ({pk_cols}) DO UPDATE SET {update_cols}
            """

        if self.mode == "bulk-insert":
            return None  # COPY handled in _flush_buffer

        raise ValueError('mode must be "insert", "upsert", or "bulk-insert"')

    def _flush_buffer(self):
        if not self.row_buffer:
            return

        try:
            if self.mode == "bulk-insert":
                with self.conn.cursor() as cur:
                    cols = ", ".join(self.columns)
                    copy_sql = f"COPY {self.table} ({cols}) FROM STDIN"
                    with cur.copy(copy_sql) as copy:
                        for row in self.row_buffer:
                            copy.write_row(row)
            else:
                if self.insert_sql:
                    self.cur.executemany(self.insert_sql, self.row_buffer)

            logging.info(f"Flushed {len(self.row_buffer)} rows ({self.mode})")
            self.row_buffer = []
            self.last_flush_time = time.time()
        except Exception as exc:
            logging.error(f"Flush failed: {exc}; reconnecting & retrying")
            self._retry_flush()

    def _time_to_flush(self) -> bool:
        return (
            time.time() - self.last_flush_time
        ) * 1000 >= self.batch_interval_ms

    def _retry_flush(self):
        try:
            self.conn.close()
            self.conn = psycopg.connect(**self.conn_params, autocommit=True)
            self.cur = self.conn.cursor()
            logging.info("Reconnected to the database...")

            if self.mode == "bulk-insert":
                with self.conn.cursor() as cur:
                    cols = ", ".join(self.columns)
                    copy_sql = f"COPY {self.table} ({cols}) FROM STDIN"
                    with cur.copy(copy_sql) as copy:
                        for row in self.row_buffer:
                            copy.write_row(row)
            else:
                if self.insert_sql:
                    self.cur.executemany(self.insert_sql, self.row_buffer)

            logging.info(
                f"Successfully flushed {len(self.row_buffer)} rows on retry."
            )
            self.row_buffer = []
            self.last_flush_time = time.time()
        except Exception as exc2:
            logging.error(f"Retry failed for {len(self.row_buffer)} rows. Raising exception.")
            self.row_buffer = []  # Clear buffer to prevent reprocessing on next batch
            raise exc2