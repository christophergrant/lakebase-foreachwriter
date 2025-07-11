import os
import uuid
import pytest
import psycopg
from psycopg.rows import tuple_row

from databricks.connect import DatabricksSession
from pyspark.sql.types import *
from datetime import date, datetime
from decimal import Decimal

from lakebase_foreachwriter.LakebaseForeachWriter import LakebaseForeachWriter

from dotenv import load_dotenv

load_dotenv()

# --- Test Configuration ---------------------------------------------------

DB_HOST = os.getenv("LAKEBASE_WRITER_HOST")
DB_USER = os.getenv("LAKEBASE_WRITER_USER")
DB_PASSWORD = os.getenv("LAKEBASE_WRITER_PASSWORD")
LAKEBASE_NAME = os.getenv("LAKEBASE_WRITER_LAKEBASE_NAME")

# Skip all tests in this module if credentials are not provided
pytestmark = pytest.mark.skipif(
    not all([DB_HOST, DB_USER, DB_PASSWORD, LAKEBASE_NAME]),
    reason="Database credentials not found in environment variables",
)

# --- Fixtures -------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    """Creates a SparkSession for the test suite."""
    return DatabricksSession.builder.serverless(True).getOrCreate()


@pytest.fixture
def db_connection():
    """Provides a psycopg connection to the database."""
    conn_params = {
        "host": DB_HOST,
        "port": 5432,
        "dbname": "databricks_postgres",
        "user": DB_USER,
        "password": DB_PASSWORD,
        "sslmode": "require",
    }
    conn = psycopg.connect(**conn_params, row_factory=tuple_row)
    yield conn
    conn.close()


@pytest.fixture
def test_table(db_connection):
    """
    A fixture that creates a uniquely named table for a test,
    and drops it after the test completes.
    """
    table_name = f"test_{str(uuid.uuid4()).replace('-', '')}"
    yield table_name
    with db_connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        db_connection.commit()


# --- Test Scenarios -------------------------------------------------------

# A list of test scenarios, each with a schema, data, and primary key for upserts.
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
        "data2": [(1, "Alice", 99.0), (3, "Charlie", 73.2)],  # Update 1, Insert 1
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
        "data2": [(1, None)],  # Update to null
        "expected": [(1, None)],
    },
}

# --- Test Schemas & Data --------------------------------------------------

# Mapping from Spark types to PostgreSQL types for CREATE TABLE statements
spark_to_pg = {
    IntegerType(): "INTEGER",
    StringType(): "TEXT",
    FloatType(): "REAL",  # Use REAL for single-precision
    LongType(): "BIGINT",
    DoubleType(): "DOUBLE PRECISION",
    BooleanType(): "BOOLEAN",
    DateType(): "DATE",
    TimestampType(): "TIMESTAMP",
    DecimalType(10, 2): "DECIMAL(10, 2)",
    # Note: ArrayType needs special handling
}

# --- Helper Functions -----------------------------------------------------


def create_table_from_schema(cursor, table_name, schema):
    """Helper to create a database table from a Spark schema."""
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
        cols.append(f'"{field.name}" {pg_type}')

    # Check for primary keys in schema metadata
    pk_cols = schema.metadata.get("primary_keys", []) if schema.metadata else []
    if pk_cols:
        cols.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

    sql = f"CREATE TABLE {table_name} ({', '.join(cols)})"
    cursor.execute(sql)


def run_writer(spark, df, mode, pk_keys, table_name):
    """Helper function to configure and run the ForeachWriter."""
    assert DB_USER is not None
    assert DB_PASSWORD is not None
    assert LAKEBASE_NAME is not None
    assert DB_HOST is not None

    writer = LakebaseForeachWriter(
        username=DB_USER,
        password=DB_PASSWORD,
        lakebase_name=LAKEBASE_NAME,
        table=table_name,
        df=df,
        mode=mode,
        primary_keys=pk_keys,
        read_write_dns=DB_HOST,
        batch_size=100,
    )
    # Simulate a micro-batch by collecting rows and processing them.
    # This avoids the complexity of managing a real stream for tests.
    writer.open(partition_id=0, epoch_id=0)
    for row in df.collect():
        writer.process(row)
    writer.close(error=None)


# --- Integration Tests ----------------------------------------------------


@pytest.mark.parametrize("mode", ["insert", "bulk-insert", "upsert"])
@pytest.mark.parametrize("scenario_name", TEST_SCENARIOS.keys())
def test_writer_modes_and_types(spark, db_connection, test_table, mode, scenario_name):
    """
    A parameterized test for all write modes and data type scenarios.
    """
    scenario = TEST_SCENARIOS[scenario_name]
    schema = scenario["schema"]
    pk = scenario["pk"]
    data1 = scenario["data1"]
    data2 = scenario["data2"]
    expected_final_data = scenario["expected"]

    # 1. Setup: Create the database table
    schema.metadata = {"primary_keys": pk}  # Add PK info for CREATE TABLE helper
    with db_connection.cursor() as cur:
        create_table_from_schema(cur, test_table, schema)
    db_connection.commit()

    # 2. Execute based on mode
    if mode == "insert":
        df1 = spark.createDataFrame(data=data1, schema=schema)
        run_writer(spark, df1, mode, pk, test_table)
        expected_rows = data1
    elif mode == "bulk-insert":
        df1 = spark.createDataFrame(data=data1, schema=schema)
        run_writer(spark, df1, mode, pk, test_table)
        expected_rows = data1
    elif mode == "upsert":
        # Initial write
        df1 = spark.createDataFrame(data=data1, schema=schema)
        run_writer(spark, df1, mode, pk, test_table)
        # Second write (updates/inserts)
        df2 = spark.createDataFrame(data=data2, schema=schema)
        run_writer(spark, df2, mode, pk, test_table)
        expected_rows = expected_final_data

    # 3. Assert: Check if data in the database is correct
    with db_connection.cursor() as cur:
        cur.execute(f'SELECT * FROM "{test_table}"')
        result_rows = cur.fetchall()

        # Sort both lists of tuples for deterministic comparison
        # The key for sorting is the primary key of the scenario
        sort_key = lambda row: tuple(row[schema.fieldNames().index(k)] for k in pk)

        # Convert Decimals to float for comparison if necessary
        def normalize_row(row):
            return tuple(float(v) if isinstance(v, Decimal) else v for v in row)

        expected_sorted = sorted(
            [normalize_row(row) for row in expected_rows], key=sort_key
        )
        result_sorted = sorted(
            [normalize_row(row) for row in result_rows], key=sort_key
        )

        assert len(result_sorted) == len(expected_sorted)
        assert result_sorted == expected_sorted
