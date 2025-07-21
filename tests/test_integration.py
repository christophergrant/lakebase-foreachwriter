import os
import uuid
from datetime import date, datetime
from decimal import Decimal

import psycopg
import pytest
from databricks.connect import DatabricksSession
from dotenv import load_dotenv
from psycopg.rows import tuple_row
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakebase_foreachwriter import LakebaseForeachWriter

load_dotenv(override=True)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Required environment variable {name} is not set")
    return value


DB_HOST = get_required_env("LAKEBASE_WRITER_HOST")
DB_USER = get_required_env("LAKEBASE_WRITER_USER")
DB_PASSWORD = get_required_env("LAKEBASE_WRITER_PASSWORD")
LAKEBASE_NAME = get_required_env("LAKEBASE_WRITER_LAKEBASE_NAME")

pytestmark = pytest.mark.skipif(
    not all([DB_HOST, DB_USER, DB_PASSWORD, LAKEBASE_NAME]),
    reason="Database credentials not found in environment variables",
)


@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.serverless(True).getOrCreate()


@pytest.fixture
def db_connection():
    conn_params = {
        "host": DB_HOST,
        "port": 5432,
        "dbname": "databricks_postgres",
        "user": DB_USER,
        "password": DB_PASSWORD,
        "sslmode": "require",
        "connect_timeout": 30,
    }
    conn = psycopg.connect(**conn_params, row_factory=tuple_row)
    yield conn
    conn.close()


@pytest.fixture
def test_table(db_connection):
    table_name = f"test_{str(uuid.uuid4()).replace('-', '')}"
    yield table_name
    with db_connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        db_connection.commit()


TEST_SCENARIOS = {
    "basic": {
        "schema": StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("score", FloatType(), True),
            ]
        ),
        "pk": ["id"],
        "data1": [(1, "Alice", 95.5), (2, "Bob", 88.0)],
        "data2": [(1, "Alice", 99.0), (3, "Charlie", 73.2)],
        "expected": [(1, "Alice", 99.0), (2, "Bob", 88.0), (3, "Charlie", 73.2)],
    },
    "all_types": {
        "schema": StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("long_col", LongType(), True),
                StructField("double_col", DoubleType(), True),
                StructField("bool_col", BooleanType(), True),
                StructField("date_col", DateType(), True),
                StructField("time_col", TimestampType(), True),
                StructField("decimal_col", DecimalType(10, 2), True),
            ]
        ),
        "pk": ["id"],
        "data1": [
            (
                1,
                10**10,
                1.1,
                True,
                date(2023, 1, 1),
                datetime(2023, 1, 1, 12, 0, 0),
                Decimal("10.10"),
            )
        ],
        "data2": [
            (
                1,
                10**11,
                2.2,
                False,
                date(2024, 1, 1),
                datetime(2024, 1, 1, 13, 0, 0),
                Decimal("20.20"),
            )
        ],
        "expected": [
            (
                1,
                10**11,
                2.2,
                False,
                date(2024, 1, 1),
                datetime(2024, 1, 1, 13, 0, 0),
                Decimal("20.20"),
            )
        ],
    },
    "with_arrays": {
        "schema": StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("int_array", ArrayType(IntegerType()), True),
                StructField("str_array", ArrayType(StringType()), True),
            ]
        ),
        "pk": ["id"],
        "data1": [(1, [1, 2], ["a", "b"])],
        "data2": [(1, [3, 4], ["c", "d"])],
        "expected": [(1, [3, 4], ["c", "d"])],
    },
    "with_nulls": {
        "schema": StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        ),
        "pk": ["id"],
        "data1": [(1, "not_null")],
        "data2": [(1, None)],
        "expected": [(1, None)],
    },
    "large_batch": {
        "schema": StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("value", StringType(), True),
            ]
        ),
        "pk": ["id"],
        "data1": [(i, f"value_{i}") for i in range(1, 501)],
        "data2": [(i, f"updated_value_{i}") for i in range(1, 251)]
        + [(i, f"new_value_{i}") for i in range(501, 601)],
        "expected": [(i, f"updated_value_{i}") for i in range(1, 251)]
        + [(i, f"value_{i}") for i in range(251, 501)]
        + [(i, f"new_value_{i}") for i in range(501, 601)],
    },
}

spark_to_pg = {
    IntegerType(): "INTEGER",
    StringType(): "TEXT",
    FloatType(): "REAL",
    LongType(): "BIGINT",
    DoubleType(): "DOUBLE PRECISION",
    BooleanType(): "BOOLEAN",
    DateType(): "DATE",
    TimestampType(): "TIMESTAMP",
    DecimalType(10, 2): "DECIMAL(10, 2)",
}


def create_table_from_schema(cursor, table_name, schema):
    cols = []
    for field in schema.fields:
        if isinstance(field.dataType, ArrayType):
            element_type = spark_to_pg.get(field.dataType.elementType)
            if not element_type:
                raise NotImplementedError(
                    f"No PG mapping for array element: {field.dataType.elementType}"
                )
            pg_type = f"{element_type}[]"
        else:
            pg_type = spark_to_pg.get(field.dataType)
            if not pg_type:
                raise NotImplementedError(
                    f"No PostgreSQL mapping for Spark type: {field.dataType}"
                )

        nullable = "NOT NULL" if not field.nullable else ""
        cols.append(f'"{field.name}" {pg_type} {nullable}'.strip())

    pk_cols = schema.metadata.get("primary_keys", []) if schema.metadata else []
    if pk_cols:
        cols.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

    sql = f"CREATE TABLE {table_name} ({', '.join(cols)})"
    cursor.execute(sql)


def run_writer(
    spark, df, mode, pk_keys, table_name, batch_size=100, batch_interval_ms=100
):
    writer = LakebaseForeachWriter(
        username=DB_USER,
        password=DB_PASSWORD,
        lakebase_name=LAKEBASE_NAME,
        table=table_name,
        df=df,
        mode=mode,
        primary_keys=pk_keys,
        host=DB_HOST,
        batch_size=batch_size,
        batch_interval_ms=batch_interval_ms,
    )

    writer.open(partition_id=0, epoch_id=0)
    for row in df.collect():
        writer.process(row)
    writer.close(error=None)


def get_sort_key(schema, pk):
    def sort_key(row):
        return tuple(row[schema.fieldNames().index(k)] for k in pk)

    return sort_key


def normalize_row(row):
    return tuple(float(v) if isinstance(v, Decimal) else v for v in row)


@pytest.mark.parametrize("mode", ["insert", "bulk-insert", "upsert"])
@pytest.mark.parametrize("scenario_name", TEST_SCENARIOS.keys())
def test_writer_modes_and_types(spark, db_connection, test_table, mode, scenario_name):
    scenario = TEST_SCENARIOS[scenario_name]
    schema = scenario["schema"]
    pk = scenario["pk"]
    data1 = scenario["data1"]
    data2 = scenario["data2"]
    expected_final_data = scenario["expected"]

    schema.metadata = {"primary_keys": pk}
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    if mode == "insert":
        df1 = spark.createDataFrame(data=data1, schema=schema)
        run_writer(spark, df1, mode, pk, test_table)
        expected_rows = data1
    elif mode == "bulk-insert":
        df1 = spark.createDataFrame(data=data1, schema=schema)
        run_writer(spark, df1, mode, pk, test_table)
        expected_rows = data1
    elif mode == "upsert":
        df1 = spark.createDataFrame(data=data1, schema=schema)
        run_writer(spark, df1, mode, pk, test_table)
        df2 = spark.createDataFrame(data=data2, schema=schema)
        run_writer(spark, df2, mode, pk, test_table)
        expected_rows = expected_final_data

    with db_connection.cursor() as cur:
        cur.execute(f'SELECT * FROM "{test_table}"')
        result_rows = cur.fetchall()

        sort_key = get_sort_key(schema, pk)
        expected_sorted = sorted(
            [normalize_row(row) for row in expected_rows], key=sort_key
        )
        result_sorted = sorted(
            [normalize_row(row) for row in result_rows], key=sort_key
        )

        assert len(result_sorted) == len(expected_sorted)
        assert result_sorted == expected_sorted


def test_batch_size_configuration(spark, db_connection, test_table):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )
    data = [(i, f"value_{i}") for i in range(1, 10)]

    schema.metadata = {"primary_keys": ["id"]}  # type: ignore[attr-defined]
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    df = spark.createDataFrame(data=data, schema=schema)
    run_writer(spark, df, "insert", ["id"], test_table, batch_size=3)

    with db_connection.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{test_table}"')
        count = cur.fetchone()[0]
        assert count == len(data)


def test_concurrent_writes_same_table(spark, db_connection, test_table):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )

    schema.metadata = {"primary_keys": ["id"]}  # type: ignore[attr-defined]
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    data1 = [(i, f"batch1_{i}") for i in range(1, 6)]
    data2 = [(i, f"batch2_{i}") for i in range(6, 11)]

    df1 = spark.createDataFrame(data=data1, schema=schema)
    df2 = spark.createDataFrame(data=data2, schema=schema)

    run_writer(spark, df1, "insert", ["id"], test_table)
    run_writer(spark, df2, "insert", ["id"], test_table)

    with db_connection.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{test_table}"')
        count = cur.fetchone()[0]
        assert count == 10


def test_empty_dataframe(spark, db_connection, test_table):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )

    schema.metadata = {"primary_keys": ["id"]}  # type: ignore[attr-defined]
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    df = spark.createDataFrame(data=[], schema=schema)
    run_writer(spark, df, "insert", ["id"], test_table)

    with db_connection.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{test_table}"')
        count = cur.fetchone()[0]
        assert count == 0


def test_sql_injection_protection(spark, db_connection, test_table):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ]
    )

    schema.metadata = {"primary_keys": ["id"]}  # type: ignore[attr-defined]
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    malicious_data = [(1, "'; DROP TABLE users; --")]
    df = spark.createDataFrame(data=malicious_data, schema=schema)
    run_writer(spark, df, "insert", ["id"], test_table)

    with db_connection.cursor() as cur:
        cur.execute(f'SELECT name FROM "{test_table}" WHERE id = 1')
        result = cur.fetchone()[0]
        assert result == "'; DROP TABLE users; --"


def test_time_based_batching(spark, db_connection, test_table):
    import time

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )

    schema.metadata = {"primary_keys": ["id"]}  # type: ignore[attr-defined]
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    writer = LakebaseForeachWriter(
        username=DB_USER,
        password=DB_PASSWORD,
        lakebase_name=LAKEBASE_NAME,
        table=test_table,
        df=spark.createDataFrame(data=[], schema=schema),
        mode="insert",
        batch_size=1000,  # Large batch size
        batch_interval_ms=50,  # Small time interval
        host=DB_HOST,
    )

    writer.open(partition_id=0, epoch_id=0)

    # Add a few rows with delays to test time-based flushing
    from pyspark.sql import Row

    writer.process(Row(id=1, value="test1"))
    time.sleep(0.06)  # Sleep longer than batch_interval_ms
    writer.process(Row(id=2, value="test2"))

    writer.close(error=None)

    with db_connection.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{test_table}"')
        count = cur.fetchone()[0]
        assert count == 2


def test_connection_error_handling(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data=[(1, "test")], schema=schema)

    writer = LakebaseForeachWriter(
        username="invalid_user",
        password="invalid_password",
        host="invalid_host",
        table="test_table",
        df=df,
        mode="insert",
    )

    result = writer.open(partition_id=0, epoch_id=0)
    assert result is False


def test_upsert_without_primary_keys(spark, db_connection, test_table):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True),
        ]
    )

    schema.metadata = {"primary_keys": ["id"]}  # type: ignore[attr-defined]
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    df = spark.createDataFrame(data=[(1, "test")], schema=schema)

    with pytest.raises(ValueError, match="primary_keys required for upsert mode"):
        writer = LakebaseForeachWriter(
            username=DB_USER,
            password=DB_PASSWORD,
            lakebase_name=LAKEBASE_NAME,
            table=test_table,
            df=df,
            mode="upsert",
            primary_keys=None,
            host=DB_HOST,
        )
        writer._build_sql()
