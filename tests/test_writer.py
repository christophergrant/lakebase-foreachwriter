import unittest
from unittest.mock import MagicMock, patch, call, ANY

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    MapType,
)

from lakebase_foreachwriter import LakebaseForeachWriter


@pytest.fixture
def mock_df():
    """Pytest fixture to create a mock DataFrame for testing."""
    df = MagicMock()
    df.schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("value", StringType(), True),
        ]
    )
    df.schema.names = ["id", "name", "value"]
    return df


class TestLakebaseForeachWriter:
    """Test suite for the LakebaseForeachWriter class."""

    def test_initialization_success(self, mock_df):
        """Tests successful initialization of the writer."""
        writer = LakebaseForeachWriter(
            username="user",
            password="pw",
            lakebase_name="test_db",
            table="test_table",
            df=mock_df,
            mode="insert",
            read_write_dns="dummy.dns",
        )
        assert writer.table == "test_table"
        assert writer.mode == "insert"
        assert writer.columns == ["id", "name", "value"]
        assert "host" in writer.conn_params

    def test_initialization_upsert_with_pk(self, mock_df):
        """Tests successful initialization in 'upsert' mode with primary keys."""
        writer = LakebaseForeachWriter(
            username="user",
            password="pw",
            lakebase_name="test_db",
            table="test_table",
            df=mock_df,
            mode="upsert",
            primary_keys=["id"],
            read_write_dns="dummy.dns",
        )
        assert writer.mode == "upsert"
        assert writer.primary_keys == ["id"]

    def test_initialization_upsert_no_pk_fails(self, mock_df):
        """Tests that 'upsert' mode raises ValueError without primary keys."""
        with pytest.raises(ValueError, match="primary_keys required for upsert mode"):
            LakebaseForeachWriter(
                username="user",
                password="pw",
                lakebase_name="test_db",
                table="test_table",
                df=mock_df,
                mode="upsert",
                read_write_dns="dummy.dns",
            )

    def test_initialization_invalid_mode_fails(self, mock_df):
        """Tests that an invalid mode raises a ValueError."""
        with pytest.raises(ValueError, match='mode must be "insert", "upsert", or "bulk-insert"'):
            LakebaseForeachWriter(
                username="user",
                password="pw",
                lakebase_name="test_db",
                table="test_table",
                df=mock_df,
                mode="invalid-mode",
                read_write_dns="dummy.dns",
            )

    def test_schema_with_unsupported_struct_type_fails(self):
        """Tests that schema with StructType raises ValueError."""
        df = MagicMock()
        df.schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("nested", StructType([StructField("a", StringType())])),
            ]
        )
        with pytest.raises(ValueError, match="unsupported complex types"):
            LakebaseForeachWriter("u", "p", "db", "t", df, read_write_dns="dummy.dns")

    def test_schema_with_unsupported_map_type_fails(self):
        """Tests that schema with MapType raises ValueError."""
        df = MagicMock()
        df.schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("map_col", MapType(StringType(), StringType())),
            ]
        )
        with pytest.raises(ValueError, match="unsupported complex types"):
            LakebaseForeachWriter("u", "p", "db", "t", df, read_write_dns="dummy.dns")

    def test_schema_with_supported_array_type_succeeds(self):
        """Tests that a schema with a simple ArrayType is supported."""
        df = MagicMock()
        df.schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("array_col", ArrayType(StringType())),
            ]
        )
        df.schema.names = ["id", "array_col"]
        # Should not raise
        LakebaseForeachWriter("u", "p", "db", "t", df, read_write_dns="dummy.dns")

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_open_method_success(self, mock_psycopg, mock_df):
        """Tests the open method successfully establishes a connection."""
        writer = LakebaseForeachWriter("u", "p", "db", "t", mock_df, read_write_dns="dummy.dns")
        result = writer.open(partition_id=0, epoch_id=0)
        assert result is True
        mock_psycopg.connect.assert_called_once_with(**writer.conn_params, autocommit=True)
        assert writer.conn is not None
        assert writer.cur is not None

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_open_method_failure(self, mock_psycopg, mock_df):
        """Tests the open method handles connection errors."""
        mock_psycopg.connect.side_effect = Exception("Connection failed")
        writer = LakebaseForeachWriter("u", "p", "db", "t", mock_df, read_write_dns="dummy.dns")
        result = writer.open(partition_id=0, epoch_id=0)
        assert result is False

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_process_and_flush_on_batch_size(self, mock_psycopg, mock_df):
        """Tests that the buffer is flushed when batch size is reached."""
        writer = LakebaseForeachWriter(
            "u", "p", "db", "t", mock_df, batch_size=2, read_write_dns="dummy.dns"
        )
        writer.open(0, 0)
        writer.process(MagicMock(__getitem__=lambda _, k: {"id": 1, "name": "a", "value": "v1"}[k]))
        writer.process(MagicMock(__getitem__=lambda _, k: {"id": 2, "name": "b", "value": "v2"}[k]))
        writer.cur.executemany.assert_called_once()
        assert len(writer.row_buffer) == 0

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.time")
    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_process_and_flush_on_time_interval(self, mock_psycopg, mock_time, mock_df):
        """Tests that the buffer is flushed when the time interval is reached."""
        mock_time.time.side_effect = [1000.0, 1000.101]  # open, process
        writer = LakebaseForeachWriter(
            "u", "p", "db", "t", mock_df, batch_interval_ms=100, read_write_dns="dummy.dns"
        )
        writer.open(0, 0)
        writer.process(MagicMock(__getitem__=lambda _, k: {"id": 1, "name": "a", "value": "v1"}[k]))
        writer.cur.executemany.assert_called_once()
        assert len(writer.row_buffer) == 0

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_close_flushes_remaining_rows(self, mock_psycopg, mock_df):
        """Tests that the close method flushes any remaining rows."""
        writer = LakebaseForeachWriter(
            "u", "p", "db", "t", mock_df, batch_size=10, read_write_dns="dummy.dns"
        )
        writer.open(0, 0)
        writer.process(MagicMock(__getitem__=lambda _, k: {"id": 1, "name": "a", "value": "v1"}[k]))
        assert len(writer.row_buffer) == 1
        writer.close(None)
        writer.cur.executemany.assert_called_once()
        assert len(writer.row_buffer) == 0
        writer.cur.close.assert_called_once()
        writer.conn.close.assert_called_once()

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_bulk_insert_mode(self, mock_psycopg, mock_df):
        """Tests the 'bulk-insert' mode using COPY."""
        writer = LakebaseForeachWriter(
            "u",
            "p",
            "db",
            "test_table",
            mock_df,
            mode="bulk-insert",
            batch_size=2,
            read_write_dns="dummy.dns",
        )
        writer.open(0, 0)

        # The bulk insert method creates its own cursor, so we mock it on the connection
        mock_cursor = writer.conn.cursor.return_value.__enter__.return_value
        mock_copy_ctx = mock_cursor.copy.return_value.__enter__.return_value

        rows = [
            (1, "a", "v1"),
            (2, "b", "v2"),
        ]
        writer.row_buffer = list(rows)  # Pre-fill buffer to test flush
        writer._flush_buffer()

        expected_sql = "COPY test_table (id, name, value) FROM STDIN"
        mock_cursor.copy.assert_called_with(expected_sql)
        mock_copy_ctx.write_row.assert_has_calls([call(row) for row in rows])

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_flush_retry_logic(self, mock_psycopg, mock_df):
        """Tests the retry mechanism for a failed flush."""
        writer = LakebaseForeachWriter(
            "u", "p", "db", "t", mock_df, batch_size=1, read_write_dns="dummy.dns"
        )
        writer.open(0, 0)

        # First call to executemany fails, second succeeds
        writer.cur.executemany.side_effect = [Exception("DB error"), None]

        writer.process(MagicMock(__getitem__=lambda _, k: {"id": 1, "name": "a", "value": "v1"}[k]))

        # executemany is called twice (initial + retry)
        assert writer.cur.executemany.call_count == 2
        # Reconnect should have been called
        assert mock_psycopg.connect.call_count == 2
        assert len(writer.row_buffer) == 0

    @patch("lakebase_foreachwriter.LakebaseForeachWriter.psycopg")
    def test_flush_retry_fails(self, mock_psycopg, mock_df):
        """Tests that an exception is raised if the retry also fails."""
        writer = LakebaseForeachWriter(
            "u", "p", "db", "t", mock_df, batch_size=1, read_write_dns="dummy.dns"
        )
        writer.open(0, 0)

        # Both calls to executemany fail
        writer.cur.executemany.side_effect = [Exception("DB error"), Exception("Retry failed")]

        with pytest.raises(Exception, match="Retry failed"):
            writer.process(
                MagicMock(__getitem__=lambda _, k: {"id": 1, "name": "a", "value": "v1"}[k])
            )
        assert len(writer.row_buffer) == 0

    @patch("databricks.sdk.WorkspaceClient")
    def test_dynamic_dns_retrieval(self, mock_ws_client, mock_df):
        """Tests that read_write_dns is fetched via the SDK if not provided."""
        # Setup mock SDK
        mock_instance = MagicMock()
        mock_instance.read_write_dns = "dynamically-fetched.dns"
        mock_ws_client.return_value.database.get_database_instance.return_value = mock_instance

        writer = LakebaseForeachWriter(
            username="user",
            password="pw",
            lakebase_name="test_db",
            table="test_table",
            df=mock_df,
        )

        mock_ws_client.return_value.database.get_database_instance.assert_called_once_with(
            "test_db"
        )
        assert writer.conn_params["host"] == "dynamically-fetched.dns" 