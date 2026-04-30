import os
import uuid
from datetime import datetime
from unittest.mock import Mock

import psycopg
import pytest
from psycopg.rows import tuple_row
from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakebase_foreachwriter import LakebaseForeachWriter

LOCAL_PG_HOST = os.getenv("LOCAL_POSTGRES_HOST", "localhost")
LOCAL_PG_PORT = int(os.getenv("LOCAL_POSTGRES_PORT", os.getenv("PGPORT", "5432")))
LOCAL_PG_USER = os.getenv(
    "LOCAL_POSTGRES_USER", os.getenv("PGUSER", os.getenv("USER", "postgres"))
)
LOCAL_PG_PASSWORD = os.getenv("LOCAL_POSTGRES_PASSWORD", os.getenv("PGPASSWORD", ""))
LOCAL_PG_DB = os.getenv("LOCAL_POSTGRES_DB", "postgres")


@pytest.fixture(scope="session")
def local_pg_params():
    params = {
        "host": LOCAL_PG_HOST,
        "port": LOCAL_PG_PORT,
        "dbname": LOCAL_PG_DB,
        "user": LOCAL_PG_USER,
        "sslmode": "disable",
        "connect_timeout": 5,
    }
    if LOCAL_PG_PASSWORD:
        params["password"] = LOCAL_PG_PASSWORD

    try:
        conn = psycopg.connect(**params)
        conn.close()
    except Exception as exc:
        pytest.skip(f"local Postgres is not available: {exc}")

    return params


@pytest.fixture
def local_pg_connection(local_pg_params):
    conn = psycopg.connect(**local_pg_params, row_factory=tuple_row)
    yield conn
    conn.close()


@pytest.fixture
def local_pg_table(local_pg_connection):
    table_name = f"test_{uuid.uuid4().hex}"
    with local_pg_connection.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                status TEXT,
                paymentmethodlastdigits TEXT,
                lastupdatedat TIMESTAMP
            )
            """
        )
    local_pg_connection.commit()

    yield table_name

    with local_pg_connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    local_pg_connection.commit()


def fake_dataframe(schema):
    df = Mock()
    df.schema = schema
    df.schema.names = schema.fieldNames()
    return df


def writer_schema():
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("status", StringType(), True),
            StructField("paymentmethodlastdigits", StringType(), True),
            StructField("lastupdatedat", TimestampType(), True),
        ]
    )


def write_rows(table_name, schema, rows, **upsert_options):
    writer = LakebaseForeachWriter(
        credential_provider=lambda: (LOCAL_PG_USER, LOCAL_PG_PASSWORD),
        table=table_name,
        df=fake_dataframe(schema),
        host=LOCAL_PG_HOST,
        mode="upsert",
        primary_keys=["id"],
        batch_size=1,
        batch_interval_ms=1,
        **upsert_options,
    )
    writer.conn_params["port"] = LOCAL_PG_PORT
    writer.conn_params["dbname"] = LOCAL_PG_DB

    assert writer.open(partition_id=0, epoch_id=0) is True
    for row in rows:
        writer.process(row)
    writer.close(error=None)


def read_row(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, status, paymentmethodlastdigits, lastupdatedat
            FROM {table_name}
            WHERE id = 1
            """
        )
        return cur.fetchone()


def test_versioned_upsert_overwrites_null_values_and_preserves_null_version(
    local_pg_connection, local_pg_table
):
    schema = writer_schema()
    t1 = datetime(2024, 1, 2, 12, 0, 0)
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    t2 = datetime(2024, 1, 3, 12, 0, 0)
    options = {
        "upsert_version_column": "lastupdatedat",
        "upsert_coalesce_columns": ["lastupdatedat"],
    }

    write_rows(
        local_pg_table,
        schema,
        [
            Row(
                id=1,
                status="authorized",
                paymentmethodlastdigits="1234",
                lastupdatedat=t1,
            )
        ],
        **options,
    )
    write_rows(
        local_pg_table,
        schema,
        [
            Row(
                id=1,
                status="finalized",
                paymentmethodlastdigits=None,
                lastupdatedat=None,
            )
        ],
        **options,
    )

    assert read_row(local_pg_connection, local_pg_table) == (1, "finalized", None, t1)

    write_rows(
        local_pg_table,
        schema,
        [Row(id=1, status="older", paymentmethodlastdigits="9999", lastupdatedat=t0)],
        **options,
    )

    assert read_row(local_pg_connection, local_pg_table) == (1, "finalized", None, t1)

    write_rows(
        local_pg_table,
        schema,
        [Row(id=1, status="settled", paymentmethodlastdigits="5678", lastupdatedat=t2)],
        **options,
    )

    assert read_row(local_pg_connection, local_pg_table) == (1, "settled", "5678", t2)


def test_upsert_coalesce_columns_preserve_existing_null_values(
    local_pg_connection, local_pg_table
):
    schema = writer_schema()
    t1 = datetime(2024, 1, 2, 12, 0, 0)
    t2 = datetime(2024, 1, 3, 12, 0, 0)
    options = {
        "upsert_version_column": "lastupdatedat",
        "upsert_coalesce_columns": ["paymentmethodlastdigits", "lastupdatedat"],
    }

    write_rows(
        local_pg_table,
        schema,
        [
            Row(
                id=1,
                status="authorized",
                paymentmethodlastdigits="1234",
                lastupdatedat=t1,
            )
        ],
        **options,
    )
    write_rows(
        local_pg_table,
        schema,
        [Row(id=1, status="settled", paymentmethodlastdigits=None, lastupdatedat=t2)],
        **options,
    )

    assert read_row(local_pg_connection, local_pg_table) == (1, "settled", "1234", t2)
