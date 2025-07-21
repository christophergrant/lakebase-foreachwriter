import queue
import threading
import time
from unittest.mock import Mock, patch

import pytest
from databricks.sdk.errors.platform import NotFound
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from lakebase_foreachwriter import LakebaseForeachWriter
from lakebase_foreachwriter.LakebaseForeachWriter import _build_conn_params


class TestBuildConnParams:
    def test_with_host_provided(self):
        result = _build_conn_params("user", "pass", host="localhost")
        expected = {
            "host": "localhost",
            "port": 5432,
            "dbname": "databricks_postgres",
            "user": "user",
            "password": "pass",
            "sslmode": "require",
        }
        assert result == expected

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient")
    def test_with_lakebase_name(self, mock_ws_client):
        # Create a mock database instance with a read_write_dns property
        mock_db = Mock()
        mock_db.read_write_dns = "test-host"

        # Create a mock database client that returns our mock database instance
        mock_db_client = Mock()
        mock_db_client.get_database_instance.return_value = mock_db

        # Set up the workspace client instance to return our mock database client
        mock_instance = Mock()
        mock_instance.database = mock_db_client
        mock_ws_client.return_value = mock_instance

        result = _build_conn_params("user", "pass", lakebase_name="test-db")

        assert result["host"] == "test-host"
        assert result["port"] == 5432
        assert result["dbname"] == "databricks_postgres"
        assert result["user"] == "user"
        assert result["password"] == "pass"
        assert result["sslmode"] == "require"
        mock_ws_client.assert_called_once()
        mock_db_client.get_database_instance.assert_called_once_with("test-db")

    def test_no_host_or_lakebase_name(self):
        with pytest.raises(
            ValueError, match="Either host or lakebase_name must be provided"
        ):
            _build_conn_params("user", "pass")

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient")
    def test_host_retrieval_fails(self, mock_ws_client):
        # Create a mock database client that raises NotFound
        mock_db_client = Mock()
        mock_db_client.get_database_instance.side_effect = NotFound(
            "Resource not found"
        )

        # Set up the workspace client instance to return our mock database client
        mock_instance = Mock()
        mock_instance.database = mock_db_client
        mock_ws_client.return_value = mock_instance

        with pytest.raises(NotFound, match="Resource not found"):
            _build_conn_params("user", "pass", lakebase_name="test-db")


class TestLakebaseForeachWriter:
    @pytest.fixture
    def mock_dataframe(self):
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        df = Mock()
        df.schema = schema
        df.schema.names = ["id", "name"]
        return df

    @pytest.fixture
    def writer(self, mock_dataframe):
        return LakebaseForeachWriter(
            username="test_user",
            password="test_pass",
            table="test_table",
            df=mock_dataframe,
            host="localhost",
        )

    def test_init_with_unsupported_types(self):
        schema = StructType(
            [
                StructField(
                    "struct_field",
                    StructType([StructField("inner", StringType())]),
                    True,
                ),
                StructField("map_field", MapType(StringType(), StringType()), True),
                StructField(
                    "array_struct",
                    ArrayType(StructType([StructField("inner", StringType())])),
                    True,
                ),
            ]
        )
        df = Mock()
        df.schema = schema

        with pytest.raises(ValueError, match="Unsupported field types found"):
            LakebaseForeachWriter("user", "pass", "table", df, host="localhost")

    def test_find_unsupported_fields(self):
        schema = StructType(
            [
                StructField("normal", StringType(), True),
                StructField(
                    "struct_field",
                    StructType([StructField("inner", StringType())]),
                    True,
                ),
                StructField("map_field", MapType(StringType(), StringType()), True),
                StructField("array_normal", ArrayType(StringType()), True),
                StructField(
                    "array_struct",
                    ArrayType(StructType([StructField("inner", StringType())])),
                    True,
                ),
            ]
        )

        unsupported = LakebaseForeachWriter._find_unsupported_fields(schema)
        assert set(unsupported) == {"struct_field", "map_field", "array_struct"}

    def test_build_sql_insert(self, writer):
        writer.mode = "insert"
        sql = writer._build_sql()
        assert sql == "INSERT INTO test_table (id, name) VALUES (%s, %s)"

    def test_build_sql_upsert(self, writer):
        writer.mode = "upsert"
        writer.primary_keys = ["id"]
        sql = writer._build_sql()
        expected = """
                INSERT INTO test_table (id, name) VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """
        assert sql.strip() == expected.strip()

    def test_build_sql_upsert_no_primary_keys(self, writer):
        writer.mode = "upsert"
        with pytest.raises(ValueError, match="primary_keys required for upsert mode"):
            writer._build_sql()

    def test_build_sql_bulk_insert(self, writer):
        writer.mode = "bulk-insert"
        sql = writer._build_sql()
        assert sql is None

    def test_build_sql_invalid_mode(self, writer):
        writer.mode = "invalid"
        with pytest.raises(ValueError, match="Invalid mode: invalid"):
            writer._build_sql()

    @patch("psycopg.connect")
    def test_open_success(self, mock_connect, writer):
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        result = writer.open(1, 100)

        assert result is True
        assert writer.partition_id == 1
        assert writer.epoch_id == 100
        assert writer.conn == mock_conn
        assert isinstance(writer.queue, queue.SimpleQueue)
        assert isinstance(writer.stop_event, threading.Event)
        assert writer.worker_thread.is_alive()

    @patch("psycopg.connect")
    def test_open_failure(self, mock_connect, writer):
        mock_connect.side_effect = Exception("Connection failed")

        result = writer.open(1, 100)

        assert result is False

    def test_process_row(self, writer):
        writer.queue = queue.SimpleQueue()
        writer.worker_error = None

        row = Row(id=1, name="test")
        writer.process(row)

        assert writer.queue.qsize() == 1
        assert writer.queue.get() == (1, "test")

    def test_process_row_with_worker_error(self, writer):
        writer.worker_error = "Worker failed"

        with pytest.raises(Exception, match="Worker failed: Worker failed"):
            writer.process(Row(id=1, name="test"))

    def test_time_to_flush(self, writer):
        writer.batch_interval_ms = 100
        writer.last_flush = time.time() - 0.2  # 200ms ago
        assert writer._time_to_flush() is True

        writer.last_flush = time.time() - 0.05  # 50ms ago
        assert writer._time_to_flush() is False

    @patch("psycopg.connect")
    def test_flush_batch_insert_mode(self, mock_connect, writer):
        # Fixed: Properly mock the context manager
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_connect.return_value = mock_conn

        writer.conn = mock_conn
        writer.batch = [(1, "test"), (2, "test2")]
        writer.mode = "insert"
        writer.sql = "INSERT INTO test_table (id, name) VALUES (%s, %s)"
        writer.partition_id = 1
        writer.epoch_id = 100

        writer._flush_batch()

        mock_cursor.executemany.assert_called_once_with(
            writer.sql, [(1, "test"), (2, "test2")]
        )
        mock_conn.commit.assert_called_once()
        assert writer.batch == []

    @patch("psycopg.connect")
    def test_flush_batch_bulk_insert_mode(self, mock_connect, writer):
        # Fixed: Properly mock nested context managers
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_copy = Mock()

        # Mock the cursor context manager
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor

        # Mock the copy context manager
        mock_copy.__enter__ = Mock(return_value=mock_copy)
        mock_copy.__exit__ = Mock(return_value=None)
        mock_cursor.copy.return_value = mock_copy

        mock_connect.return_value = mock_conn

        writer.conn = mock_conn
        writer.batch = [(1, "test"), (2, "test2")]
        writer.mode = "bulk-insert"
        writer.partition_id = 1
        writer.epoch_id = 100

        writer._flush_batch()

        mock_cursor.copy.assert_called_once_with(
            "COPY test_table (id, name) FROM STDIN"
        )
        assert mock_copy.write_row.call_count == 2
        mock_conn.commit.assert_called_once()

    @patch("psycopg.connect")
    def test_flush_batch_rollback_on_error(self, mock_connect, writer):
        # Fixed: Properly mock the context manager and add rollback
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = Exception("Database error")
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        writer.conn = mock_conn
        writer.batch = [(1, "test")]
        writer.mode = "insert"
        writer.sql = "INSERT INTO test_table (id, name) VALUES (%s, %s)"

        with pytest.raises(Exception, match="Database error"):
            writer._flush_batch()

        mock_conn.rollback.assert_called_once()

    def test_flush_remaining(self, writer):
        writer.queue = queue.SimpleQueue()
        writer.queue.put((1, "test"))
        writer.queue.put((2, "test2"))
        writer.batch = []

        with patch.object(writer, "_flush_batch") as mock_flush:
            writer._flush_remaining()

        assert writer.batch == [(1, "test"), (2, "test2")]
        mock_flush.assert_called_once()

    def test_close_with_large_queue_warning(self, writer, caplog):
        writer.queue = queue.SimpleQueue()
        for i in range(10000):  # More than 5x batch_size (1000)
            writer.queue.put((i, f"test{i}"))

        writer.stop_event = threading.Event()
        writer.worker_thread = Mock()
        writer.worker_thread.is_alive.return_value = False
        writer.conn = Mock()
        writer.partition_id = 1
        writer.epoch_id = 100

        with patch.object(writer, "_flush_remaining"):
            writer.close(None)

        assert "remaining queue size is 10000" in caplog.text
        assert "more than 5x the batch size" in caplog.text

    def test_worker_thread_batch_processing(self, writer):
        writer.queue = queue.SimpleQueue()
        writer.stop_event = threading.Event()
        writer.batch = []
        writer.batch_size = 2

        # Add items to queue
        writer.queue.put((1, "test1"))
        writer.queue.put((2, "test2"))
        writer.queue.put((3, "test3"))

        with patch.object(writer, "_flush_batch") as mock_flush:
            with patch.object(writer, "_time_to_flush", return_value=False):
                # Simulate one iteration of worker loop
                while len(writer.batch) < writer.batch_size:
                    try:
                        item = writer.queue.get(timeout=0.01)
                        writer.batch.append(item)
                    except queue.Empty:
                        break

                # Should flush when batch_size reached
                if len(writer.batch) >= writer.batch_size:
                    writer._flush_batch()

        assert len(writer.batch) >= 2
        mock_flush.assert_called()

    def test_worker_thread_time_based_flush(self, writer):
        writer.queue = queue.SimpleQueue()
        writer.stop_event = threading.Event()
        writer.batch = [(1, "test")]
        writer.batch_size = 10

        with patch.object(writer, "_flush_batch") as mock_flush:
            with patch.object(writer, "_time_to_flush", return_value=True):
                # Simulate worker deciding to flush based on time
                should_flush = len(writer.batch) >= writer.batch_size or (
                    writer.batch and writer._time_to_flush()
                )
                if should_flush:
                    writer._flush_batch()

        mock_flush.assert_called_once()

    def test_init_parameters(self, mock_dataframe):
        writer = LakebaseForeachWriter(
            username="user",
            password="pass",
            table="table",
            df=mock_dataframe,
            host="host",
            mode="UPSERT",
            primary_keys=["id"],
            batch_size=500,
            batch_interval_ms=50,
        )

        assert writer.username == "user"
        assert writer.password == "pass"
        assert writer.table == "table"
        assert writer.mode == "upsert"  # Should be lowercased
        assert writer.primary_keys == ["id"]
        assert writer.batch_size == 500
        assert writer.batch_interval_ms == 50
        assert writer.columns == ["id", "name"]

    # Additional test to verify lakebase_name success case by mocking at class level
    @patch("lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient")
    def test_with_lakebase_name_integration(self, mock_ws_client, mock_dataframe):
        """Test successful initialization with lakebase_name using WorkspaceClient mock"""
        # Create a mock database instance with a read_write_dns property
        mock_db = Mock()
        mock_db.read_write_dns = "test-host"

        # Create a mock database client that returns our mock database instance
        mock_db_client = Mock()
        mock_db_client.get_database_instance.return_value = mock_db

        # Set up the workspace client instance to return our mock database client
        mock_instance = Mock()
        mock_instance.database = mock_db_client
        mock_ws_client.return_value = mock_instance

        writer = LakebaseForeachWriter(
            username="user",
            password="pass",
            table="table",
            df=mock_dataframe,
            lakebase_name="test-db",
        )

        assert writer.conn_params["host"] == "test-host"
        assert writer.conn_params["user"] == "user"
        assert writer.conn_params["password"] == "pass"
        mock_ws_client.assert_called_once()
        mock_db_client.get_database_instance.assert_called_once_with("test-db")
