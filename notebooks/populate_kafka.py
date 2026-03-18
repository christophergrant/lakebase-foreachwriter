# Databricks notebook source
# MAGIC %md
# MAGIC # Populate Kafka Topic for RTM Latency Test
# MAGIC
# MAGIC Writes test rows to Kafka at a steady rate using the rate source.
# MAGIC Runs for a fixed duration, then exits. The RTM consumer job reads from this topic.

# COMMAND ----------

dbutils.widgets.text("variant", "FIXED", "BUGGY or FIXED")
dbutils.widgets.text("duration_minutes", "5", "How long to produce")
dbutils.widgets.text("rows_per_second", "2000", "Rate of row generation")

VARIANT = dbutils.widgets.get("variant").upper()
DURATION_MINUTES = int(dbutils.widgets.get("duration_minutes"))
ROWS_PER_SECOND = int(dbutils.widgets.get("rows_per_second"))

print(f"Producing to Kafka: variant={VARIANT}, duration={DURATION_MINUTES}min, rate={ROWS_PER_SECOND}/sec")

# COMMAND ----------

import time
from pyspark.sql.functions import col, lit, to_json, struct

KAFKA_BOOTSTRAP = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers-tls")
KAFKA_TOPIC = f"ar113790-rtm-latency-{VARIANT.lower()}"

# Clear stale checkpoint
ckpt_path = f"/tmp/rtm_kafka_producer_ckpt/{VARIANT.lower()}"
try:
    dbutils.fs.rm(ckpt_path, recurse=True)
except Exception:
    pass

# Write rows to Kafka. Each row is JSON with id, pipeline_ts, variant.
query = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .load()
    .select(
        col("value").cast("string").alias("key"),
        to_json(struct(
            col("value").alias("id"),
            col("timestamp").alias("pipeline_ts"),
            lit(VARIANT).alias("variant"),
        )).alias("value"),
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", "SSL")
    .option("topic", KAFKA_TOPIC)
    .option("checkpointLocation", ckpt_path)
    .trigger(processingTime="1 second")
    .start()
)

# Run for the configured duration
run_seconds = DURATION_MINUTES * 60
start = time.time()
while time.time() - start < run_seconds:
    if not query.isActive:
        print(f"Producer stopped unexpectedly!")
        break
    time.sleep(10)

query.stop()
total_time = time.time() - start
print(f"Producer stopped after {total_time:.0f}s. Wrote ~{int(ROWS_PER_SECOND * total_time):,} rows to {KAFKA_TOPIC}")
