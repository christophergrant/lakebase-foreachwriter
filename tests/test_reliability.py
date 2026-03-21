import time
from importlib import import_module
from unittest.mock import Mock

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from lakebase_foreachwriter import LakebaseForeachWriter
from tests.fakes.fake_psycopg import (
    configure_fake_psycopg,
    ensure_fake_table,
    get_fake_row_count,
    get_fake_rows,
    reset_fake_psycopg,
)

writer_module = import_module("lakebase_foreachwriter.LakebaseForeachWriter")


def _wait_for(predicate, timeout: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


@pytest.fixture
def mock_dataframe():
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


@pytest.fixture(autouse=True)
def fake_psycopg_backend(monkeypatch):
    reset_fake_psycopg()
    from tests.fakes import fake_psycopg

    monkeypatch.setattr(writer_module.psycopg, "connect", fake_psycopg.connect)
    yield
    reset_fake_psycopg()


def test_close_drains_remaining_rows(mock_dataframe):
    ensure_fake_table("test_table")
    writer = LakebaseForeachWriter(
        username="user",
        password="pass",
        table="test_table",
        df=mock_dataframe,
        host="fake-host",
        batch_size=100,
        batch_interval_ms=10000,
    )

    assert writer.open(0, 0) is True
    writer.process(Row(id=1, name="alpha"))
    writer.process(Row(id=2, name="beta"))
    writer.process(Row(id=3, name="gamma"))
    writer.close(None)

    assert get_fake_row_count("test_table") == 3


def test_worker_error_surfaces_to_process_and_preserves_failed_batch(
    mock_dataframe,
):
    ensure_fake_table("test_table")
    configure_fake_psycopg(fail_on_executemany_call=1)
    writer = LakebaseForeachWriter(
        username="user",
        password="pass",
        table="test_table",
        df=mock_dataframe,
        host="fake-host",
        batch_size=1,
        batch_interval_ms=1,
    )

    assert writer.open(0, 0) is True
    writer.process(Row(id=1, name="first"))
    assert _wait_for(lambda: writer.worker_error is not None)

    with pytest.raises(Exception, match="Worker failed"):
        writer.process(Row(id=2, name="second"))

    writer.close(None)
    assert get_fake_row_count("test_table") == 1


def test_slow_database_does_not_hang_close(mock_dataframe):
    ensure_fake_table("test_table")
    configure_fake_psycopg(execute_base_ms=50, commit_ms=10)
    writer = LakebaseForeachWriter(
        username="user",
        password="pass",
        table="test_table",
        df=mock_dataframe,
        host="fake-host",
        batch_size=1,
        batch_interval_ms=1,
    )

    assert writer.open(0, 0) is True
    for idx in range(5):
        writer.process(Row(id=idx, name=f"name-{idx}"))

    start = time.time()
    writer.close(None)
    elapsed = time.time() - start

    assert elapsed < 2.0
    assert get_fake_row_count("test_table") == 5


def test_upsert_keeps_latest_row(mock_dataframe):
    ensure_fake_table("test_table", primary_keys=("id",))
    writer = LakebaseForeachWriter(
        username="user",
        password="pass",
        table="test_table",
        df=mock_dataframe,
        host="fake-host",
        mode="upsert",
        primary_keys=["id"],
        batch_size=2,
        batch_interval_ms=1,
    )

    assert writer.open(0, 0) is True
    writer.process(Row(id=1, name="first"))
    writer.process(Row(id=1, name="latest"))
    writer.close(None)

    rows = get_fake_rows("test_table")
    assert len(rows) == 1
    assert rows[0]["name"] == "latest"


def test_bulk_insert_persists_rows(mock_dataframe):
    ensure_fake_table("test_table")
    writer = LakebaseForeachWriter(
        username="user",
        password="pass",
        table="test_table",
        df=mock_dataframe,
        host="fake-host",
        mode="bulk-insert",
        batch_size=2,
        batch_interval_ms=1,
    )

    assert writer.open(0, 0) is True
    writer.process(Row(id=1, name="one"))
    writer.process(Row(id=2, name="two"))
    writer.process(Row(id=3, name="three"))
    writer.close(None)

    assert get_fake_row_count("test_table") == 3
