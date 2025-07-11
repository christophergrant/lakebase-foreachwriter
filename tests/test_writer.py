import pytest
from unittest.mock import MagicMock, call

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    MapType,
)

from lakebase_foreachwriter.LakebaseForeachWriter import (
    LakebaseForeachWriter,
    _build_conn_params,
)


@pytest.fixture
def mock_df():
    mock = MagicMock()
    mock.schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
        ]
    )
    mock.schema.names = ["id", "name"]
    return mock


class TestBuildConnParams:
    def test_build_conn_params_with_lakebase_name(self, mocker):
        mock_ws_client = mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient"
        )
        mock_ws_client.return_value.database.get_database_instance.return_value.read_write_dns = "test-dns"
        params = _build_conn_params(
            user="test_user", password="test_password", lakebase_name="test_lakebase"
        )
        assert params["host"] == "test-dns"
        assert params["user"] == "test_user"
        mock_ws_client.return_value.database.get_database_instance.assert_called_with(
            "test_lakebase"
        )

    def test_build_conn_params_with_read_write_dns(self):
        params = _build_conn_params(
            user="test_user",
            password="test_password",
            read_write_dns="provided-dns",
        )
        assert params["host"] == "provided-dns"

    def test_build_conn_params_value_error(self):
        with pytest.raises(ValueError):
            _build_conn_params(user="user", password="password")


class TestLakebaseForeachWriter:
    def test_init_insert_mode(self, mocker, mock_df):
        mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient"
        ).return_value.database.get_database_instance.return_value.read_write_dns = (
            "dns"
        )
        writer = LakebaseForeachWriter(
            username="user",
            password="password",
            table="my_table",
            df=mock_df,
            lakebase_name="my_lakebase",
            mode="insert",
        )
        assert writer.mode == "insert"
        assert "INSERT INTO my_table" in writer.insert_sql

    def test_init_upsert_mode(self, mocker, mock_df):
        mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient"
        ).return_value.database.get_database_instance.return_value.read_write_dns = (
            "dns"
        )
        writer = LakebaseForeachWriter(
            username="user",
            password="password",
            table="my_table",
            df=mock_df,
            lakebase_name="my_lakebase",
            mode="upsert",
            primary_keys=["id"],
        )
        assert writer.mode == "upsert"
        assert "ON CONFLICT (id) DO UPDATE" in writer.insert_sql

    def test_init_upsert_mode_no_pk_raises_error(self, mocker, mock_df):
        mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient"
        ).return_value.database.get_database_instance.return_value.read_write_dns = (
            "dns"
        )
        with pytest.raises(ValueError):
            LakebaseForeachWriter(
                username="user",
                password="password",
                table="my_table",
                df=mock_df,
                lakebase_name="my_lakebase",
                mode="upsert",
            )

    def test_init_bulk_insert_mode(self, mocker, mock_df):
        mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient"
        ).return_value.database.get_database_instance.return_value.read_write_dns = (
            "dns"
        )
        writer = LakebaseForeachWriter(
            username="user",
            password="password",
            table="my_table",
            df=mock_df,
            lakebase_name="my_lakebase",
            mode="bulk-insert",
        )
        assert writer.mode == "bulk-insert"
        assert writer.insert_sql is None

    def test_init_no_lakebase_or_dns_raises_error(self, mock_df):
        with pytest.raises(ValueError):
            LakebaseForeachWriter(
                username="user",
                password="password",
                table="my_table",
                df=mock_df,
            )

    def test_init_unsupported_type_raises_error(self, mocker, mock_df):
        mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.WorkspaceClient"
        ).return_value.database.get_database_instance.return_value.read_write_dns = (
            "dns"
        )
        mock_df.schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("data", MapType(StringType(), StringType()), False),
            ]
        )
        with pytest.raises(ValueError):
            LakebaseForeachWriter(
                username="user",
                password="password",
                table="my_table",
                df=mock_df,
                lakebase_name="my_lakebase",
            )

    def test_open_process_close(self, mocker, mock_df):
        mock_psycopg = mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.psycopg"
        )
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        writer = LakebaseForeachWriter(
            username="user",
            password="password",
            table="my_table",
            df=mock_df,
            read_write_dns="dns",
            mode="insert",
            batch_size=2,
        )

        # open
        opened = writer.open(0, 0)
        assert opened
        mock_psycopg.connect.assert_called_once()
        mock_conn.cursor.assert_called_once()
        assert writer.conn is not None
        assert writer.cur is not None

        # process
        row1 = MagicMock()
        row1.__getitem__.side_effect = lambda key: {"id": 1, "name": "a"}[key]
        row2 = MagicMock()
        row2.__getitem__.side_effect = lambda key: {"id": 2, "name": "b"}[key]
        row3 = MagicMock()
        row3.__getitem__.side_effect = lambda key: {"id": 3, "name": "c"}[key]

        writer.process(row1)
        assert len(writer.row_buffer) == 1
        mock_cur.executemany.assert_not_called()

        writer.process(row2)
        assert len(writer.row_buffer) == 0
        mock_cur.executemany.assert_called_once()
        assert mock_cur.executemany.call_args[0][1] == [(1, "a"), (2, "b")]

        writer.process(row3)
        assert len(writer.row_buffer) == 1

        # close
        writer.close(None)
        mock_cur.executemany.assert_has_calls(
            [
                call(writer.insert_sql, [(1, "a"), (2, "b")]),
                call(writer.insert_sql, [(3, "c")]),
            ]
        )
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_flush_bulk_insert(self, mocker, mock_df):
        mock_psycopg = mocker.patch(
            "lakebase_foreachwriter.LakebaseForeachWriter.psycopg"
        )
        mock_conn = MagicMock()
        mock_cur_open = MagicMock()
        mock_cur_flush = MagicMock()
        mock_copy = MagicMock()
        mock_psycopg.connect.return_value = mock_conn

        mock_flush_cm = MagicMock()
        mock_flush_cm.__enter__.return_value = mock_cur_flush

        mock_conn.cursor.side_effect = [mock_cur_open, mock_flush_cm]
        mock_cur_flush.copy.return_value.__enter__.return_value = mock_copy

        writer = LakebaseForeachWriter(
            username="user",
            password="password",
            table="my_table",
            df=mock_df,
            read_write_dns="dns",
            mode="bulk-insert",
            batch_size=2,
        )
        writer.open(0, 0)

        row1 = MagicMock()
        row1.__getitem__.side_effect = lambda key: {"id": 1, "name": "a"}[key]

        writer.process(row1)
        writer.close(None)

        mock_cur_flush.copy.assert_called_with("COPY my_table (id, name) FROM STDIN")
        mock_copy.write_row.assert_called_once_with((1, "a"))
        mock_cur_open.close.assert_called_once()
