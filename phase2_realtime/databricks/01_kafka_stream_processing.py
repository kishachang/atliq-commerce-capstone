# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # AtliQ Phase 2 (LEARNER STARTER) — Kafka → Delta with Structured Streaming
# MAGIC Complete the TODOs to build Bronze → Silver → Gold as **streams**.
# MAGIC
# MAGIC **Free Edition (serverless) rules:** checkpoints go in a Unity Catalog
# MAGIC **Volume** (no DBFS), streams write to **managed tables**, and every stream
# MAGIC needs its **own** checkpoint folder.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

KAFKA_BOOTSTRAP = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"   # <-- yours
KAFKA_API_KEY   = "PICY2KM6DDCVMXEH"
KAFKA_API_SECRET = "cfltr7a6GRqSFXcIrbhHFJ//PK6gNdQ6G1vZABrfC/Nxq7CEpnfS+hjs9/F3pL9A"
TOPIC = "atliq.orders.events"

# COMMAND ----------

CATALOG = "dbw_atliq_capstone"
SCHEMA = "streaming"

CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Catalog already exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints"
)

# COMMAND ----------

kafka_jaas = f"""
kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required
username="{KAFKA_API_KEY}"
password="{KAFKA_API_SECRET}";
"""

# COMMAND ----------

# MAGIC %md ## TASK 1 — Bronze: raw events off Kafka, no parsing
# MAGIC Read the topic with `spark.readStream.format("kafka")` and append the raw
# MAGIC records to `atliq.streaming.bronze_order_events`.
# MAGIC
# MAGIC Hints:
# MAGIC - Options you need: `kafka.bootstrap.servers`, `subscribe`,
# MAGIC   `startingOffsets = earliest`, `kafka.security.protocol = SASL_SSL`,
# MAGIC   `kafka.sasl.mechanism = PLAIN`, and `kafka.sasl.jaas.config`
# MAGIC   (on Databricks the login module class is
# MAGIC   `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`).
# MAGIC - Kafka gives you binary key/value — CAST both to STRING.
# MAGIC - Keep topic, partition, offset, timestamp columns too. Bronze keeps everything.
# MAGIC - writeStream: outputMode "append", checkpointLocation f"{CKPT}/bronze",
# MAGIC   .toTable(...)

# COMMAND ----------

# DBTITLE 1,Cell 8
# TODO: Task 1 — your Bronze stream here
kafka_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", kafka_jaas)
        .load()
)

#Converting binary kafka key/value into strings
bronze_df = kafka_stream.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),
    "topic",
    "partition",
    "offset",
    "timestamp"
)

#Starting Stream
bronze_query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/bronze")
        .trigger(availableNow=True)
        .toTable("dbw_atliq_capstone.streaming.bronze_order_events")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dbw_atliq_capstone.streaming.bronze_order_events
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md ## TASK 2 — Silver: parse, de-duplicate, handle late data
# MAGIC Stream FROM the Bronze table into `atliq.streaming.silver_order_events`:
# MAGIC 1. Parse the JSON value with an explicit schema (event_id, event_type,
# MAGIC    event_ts, order_id, customer_id, city, product_id, quantity,
# MAGIC    order_amount, payment_method).
# MAGIC 2. Convert event_ts to a real timestamp.
# MAGIC 3. Add a **10-minute watermark** on event_ts, then
# MAGIC    **dropDuplicates(["event_id"])** — so a replayed event can never land twice.
# MAGIC
# MAGIC Hint: `spark.readStream.table(...)`, `F.from_json`, `withWatermark`.

# COMMAND ----------

# TODO: Task 2 — your Silver stream here
event_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("order_id", T.LongType()),
    T.StructField("customer_id", T.LongType()),
    T.StructField("city", T.StringType()),
    T.StructField("product_id", T.IntegerType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("order_amount", T.DoubleType()),
    T.StructField("payment_method", T.StringType()),
])

# COMMAND ----------

#Reading Bronze as stream
bronze_stream = spark.readStream.table(
    "dbw_atliq_capstone.streaming.bronze_order_events"
)

# COMMAND ----------

#Parsing and wring silver
silver_df = (
    bronze_stream
        .select(
            F.from_json("value", event_schema).alias("event"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .select(
            "event.*",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp"
        )
        .withColumn(
            "event_ts",
            F.to_timestamp("event_ts")
        )
        .withWatermark(
            "event_ts",
            "10 minutes"
        )
        .dropDuplicates(["event_id"])
)

silver_query = (
    silver_df.writeStream.trigger(availableNow=True)
.format("delta")
.outputMode("append")
.option("checkpointLocation", f"{CKPT}/silver")
.toTable("dbw_atliq_capstone.streaming.silver_order_events")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     event_type,
# MAGIC     COUNT(*) AS events
# MAGIC FROM dbw_atliq_capstone.streaming.silver_order_events
# MAGIC GROUP BY event_type;

# COMMAND ----------

# MAGIC %md ## TASK 3 — Gold: the live revenue ticker
# MAGIC From the Silver stream, keep only `payment_received` events and aggregate
# MAGIC into **5-minute tumbling windows**: orders_paid = count, revenue = sum of
# MAGIC order_amount. Append closed windows to `atliq.streaming.gold_revenue_5min`.
# MAGIC
# MAGIC Hint: `F.window("event_ts", "5 minutes")` — and think about WHY a window
# MAGIC only appears after the watermark passes its end (you will explain this
# MAGIC in your write-up).

# COMMAND ----------

# TODO: Task 3 — your Gold stream here
silver_stream = (
    spark.readStream
        .table("dbw_atliq_capstone.streaming.silver_order_events")
        .withWatermark("event_ts", "10 minutes")
)

# COMMAND ----------

# DBTITLE 1,Cell 17
#Building and Writing Gold
gold_df = (
    silver_stream
        .filter(F.col("event_type") == "payment_received")
        .groupBy(
            F.window("event_ts", "5 minutes").alias("window")
        )
        .agg(
            F.count("*").alias("orders_paid"),
            F.sum("order_amount").alias("revenue")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "orders_paid",
            "revenue"
        )
)


gold_query = (
    gold_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/gold")
        .trigger(availableNow=True)
        .toTable("dbw_atliq_capstone.streaming.gold_revenue_5min")
)

# COMMAND ----------

# MAGIC %md ## Verify (given)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS events
# MAGIC FROM dbw_atliq_capstone.streaming.silver_order_events GROUP BY event_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbw_atliq_capstone.streaming.gold_revenue_5min ORDER BY window_start DESC LIMIT 12;