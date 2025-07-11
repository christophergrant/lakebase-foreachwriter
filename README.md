# LakebaseForeachWriter

A simple PySpark `ForeachWriter` for writing streaming DataFrames to a Lakebase database. It is designed to be used by **[ real-time mode](https://www.databricks.com/dataaisummit/session/real-time-mode-technical-deep-dive-how-we-built-sub-300-millisecond)**, but works in microbatch-mode as well.

This writer is designed for structured streaming jobs and provides a straightforward way to sink data into a database table. It supports insert, upsert, and high-performance bulk-insert modes.

## Features

-   **Multiple Write Modes**:
    -   `insert`: Standard `INSERT` statements.
    -   `upsert`: Performs an `INSERT ... ON CONFLICT DO UPDATE`, requires specifying primary keys.
    -   `bulk-insert`: Uses PostgreSQL's highly efficient `COPY` command for maximum throughput.
-   **Automatic DNS Resolution**: Can dynamically resolve the Lakebase DNS using the Databricks SDK if you provide a `lakebase_name`.
-   **Batching**: Buffers rows and flushes them in batches based on size or time intervals to optimize database performance.
-   **Resilience**: Includes a simple retry mechanism for transient connection failures.
-   **Self-Contained**: Does not require any third-party libraries outside of `pyspark` and `psycopg`.

## Usage

A complete example of how to use `LakebaseForeachWriter` can be found in the examples below. The basic setup involves creating a streaming DataFrame and then passing the writer instance to the `.foreach()` sink in your `writeStream` query.

## Examples

The following examples assume you have a `streaming_df` DataFrame ready to be written and that the required connection parameters (`LAKEBASE_USER`, `LAKEBASE_PASSWORD`, and either `LAKEBASE_NAME` or a DNS name) have been defined as variables in your notebook scope.

### Example 1: `insert` mode

This is the default mode. It performs a standard SQL `INSERT` for each row. It's reliable but can be slow for very large volumes of data. This example connects by providing the DNS directly.

```python
from lakebase_foreachwriter import LakebaseForeachWriter

writer = LakebaseForeachWriter(
    username=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD,
    read_write_dns="your-lakebase.dns.databricks.com",  # Directly provide the DNS
    table="my_target_table",
    df=streaming_df,
    mode="insert"  # This is the default, but explicitly shown here
)

query = (
    streaming_df.writeStream
    .foreach(writer)
    .option("checkpointLocation", "/tmp/spark_checkpoints/insert_example")
    .start()
)
```

### Example 2: `upsert` mode

This mode performs an `INSERT ... ON CONFLICT DO UPDATE`. It's useful for sinking data that may contain duplicates or records that need to be updated in place. This example connects by providing the `lakebase_name` for dynamic DNS lookup.

**This mode requires the `primary_keys` parameter** to be set so the database knows how to identify conflicts.

```python
from lakebase_foreachwriter import LakebaseForeachWriter

writer = LakebaseForeachWriter(
    username=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD,
    lakebase_name=LAKEBASE_NAME,  # Use the lakebase name for dynamic lookup
    table="my_target_table",
    df=streaming_df,
    mode="upsert",
    primary_keys=["id"]  # Must be provided for upsert
)

query = (
    streaming_df.writeStream
    .foreach(writer)
    .option("checkpointLocation", "/tmp/spark_checkpoints/upsert_example")
    .start()
)
```

### Example 3: `bulk-insert` mode

This is the highest-performance mode, ideal for high-throughput streaming jobs. It uses PostgreSQL's `COPY` command to write many rows at once from an in-memory buffer, which is significantly faster than individual `INSERT` statements.

This mode does not require any special parameters. This example connects by providing the DNS directly.

```python
from lakebase_foreachwriter import LakebaseForeachWriter

writer = LakebaseForeachWriter(
    username=LAKEBASE_USER,
    password=LAKEBASE_PASSWORD,
    read_write_dns="your-lakebase.dns.databricks.com",  # Directly provide the DNS
    table="my_target_table",
    df=streaming_df,
    mode="bulk-insert"
)

query = (
    streaming_df.writeStream
    .foreach(writer)
    .option("checkpointLocation", "/tmp/spark_checkpoints/bulk_insert_example")
    .start()
)
```

## Deploying this code

At this time, go to [`src/lakebase_foreachwriter/LakebaseForeachWriter.py`](src/lakebase_foreachwriter/LakebaseForeachWriter.py) and copy the code there.

## Key Parameters

-   `username` & `password`: Your database credentials. Use a service principal for production workloads. This could be a database entity, service principal, or Databricks user.
-   `lakebase_name`: The name of your Lakebase instance in Databricks. The writer will use this to automatically find the correct database endpoint. **You must provide either `lakebase_name` or `read_write_dns`.**
-   `read_write_dns`: If you want to bypass the automatic lookup, you can provide the full DNS for the read-write endpoint directly. **You must provide either `lakebase_name` or `read_write_dns`.**
-   `table`: The name of the database table you are writing to, optionally including the schema in form schema.table.
-   `df`: The DataFrame being written. Its schema is used to configure the writer. Note that complex types such as `struct` and `map` are not supported and will throw an error.
-   `mode`: The write strategy.
    -   `"insert"`: Appends all rows.
    -   `"upsert"`: Inserts new rows or updates existing ones based on `primary_keys`.
    -   `"bulk-insert"`: Uses a high-performance `COPY` operation, ideal for large volumes of data.
-   `primary_keys`: A list of column names that form the primary key for the table. This is **required** for `"upsert"` mode.
-   `batch_size`: The number of rows to buffer before writing to the database.
-   `batch_interval_ms`: The maximum time to wait before flushing the buffer, even if it's not full. 


## Other comments

- It is recommended to create roles with defined passwords, as this connector does not support native Oauth token refresh. Something like this:
```sql
CREATE ROLE "serviceprincipal"
  WITH LOGIN
       PASSWORD 'PASSWORD'
;
```
- The alternative to having Structured Streaming write to Postgres is to use foreachBatch and Spark's batch JDBC writer within it. This works well but will have higher latency than real-time mode with this foreach writer approach. Also, this writer supports upserts and bulk inserts via copy.