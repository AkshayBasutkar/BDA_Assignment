"""
Read flight events from Kafka, compute simple streaming aggregates,
and write them to CSV, PostgreSQL, or both.
"""

import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, max as spark_max, round as spark_round, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "air-traffic-raw")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./checkpoints")
SINK = os.getenv("SINK", "csv").lower()
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "air_traffic")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

if SINK not in {"csv", "postgres", "both"}:
    raise ValueError("SINK must be csv, postgres, or both")

if SINK in {"csv", "both"}:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

spark = (
    SparkSession.builder.appName("AirTrafficAnalytics")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("source", StringType()),
        StructField("flight_id", StringType()),
        StructField("icao24", StringType()),
        StructField("origin_country", StringType()),
        StructField("timestamp", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("altitude_ft", IntegerType()),
        StructField("speed_kmh", IntegerType()),
        StructField("heading_deg", IntegerType()),
        StructField("status", StringType()),
        StructField("origin", StringType()),
        StructField("destination", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

flights = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

live_counts = flights.groupBy(window("event_time", "1 minute"), col("status")).agg(
    count("*").alias("flight_count")
)

airline_perf = (
    flights.withColumn("airline", col("flight_id").substr(1, 2))
    .groupBy(window("event_time", "2 minutes"), col("airline"))
    .agg(
        count("*").alias("updates"),
        spark_round(avg("speed_kmh"), 1).alias("avg_speed_kmh"),
        spark_max("altitude_ft").alias("max_altitude_ft"),
    )
)

airport_traffic = (
    flights.filter(col("origin").isNotNull())
    .groupBy(window("event_time", "1 minute"), col("origin"))
    .agg(count("*").alias("departures"))
)


def postgres_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def write_csv(batch_df, name, mode="overwrite"):
    batch_df.write.mode(mode).option("header", True).csv(f"{OUTPUT_DIR}/{name}")


def write_postgres(batch_df, table, columns, replace=False):
    rows = [tuple(row[column] for column in columns) for row in batch_df.select(*columns).collect()]
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            if replace:
                cursor.execute(f"TRUNCATE TABLE {table}")
            if rows:
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
                execute_values(cursor, query, rows)


def write_batch(batch_df, name, table, columns, replace=False, csv_mode="overwrite"):
    if SINK in {"csv", "both"}:
        write_csv(batch_df, name, mode=csv_mode)
    if SINK in {"postgres", "both"}:
        write_postgres(batch_df, table, columns, replace=replace)
    print(f"[batch] wrote {name}")


def write_flight_events(batch_df, _batch_id):
    flight_events_df = batch_df.select(
        "source",
        "flight_id",
        "icao24",
        "origin_country",
        col("event_time"),
        "latitude",
        "longitude",
        "altitude_ft",
        "speed_kmh",
        "heading_deg",
        "status",
        "origin",
        "destination",
    )
    write_batch(
        flight_events_df,
        "flight_events",
        "flight_events",
        [
            "source",
            "flight_id",
            "icao24",
            "origin_country",
            "event_time",
            "latitude",
            "longitude",
            "altitude_ft",
            "speed_kmh",
            "heading_deg",
            "status",
            "origin",
            "destination",
        ],
        csv_mode="append",
    )


def write_live_counts(batch_df, _batch_id):
    result_df = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "status",
        "flight_count",
    )
    write_batch(
        result_df,
        "live_counts",
        "live_counts",
        ["window_start", "window_end", "status", "flight_count"],
        replace=True,
    )


def write_airline_perf(batch_df, _batch_id):
    result_df = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "airline",
        "updates",
        "avg_speed_kmh",
        "max_altitude_ft",
    )
    write_batch(
        result_df,
        "airline_perf",
        "airline_perf",
        [
            "window_start",
            "window_end",
            "airline",
            "updates",
            "avg_speed_kmh",
            "max_altitude_ft",
        ],
        replace=True,
    )


def write_airport_traffic(batch_df, _batch_id):
    result_df = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "origin",
        "departures",
    )
    write_batch(
        result_df,
        "airport_traffic",
        "airport_traffic",
        ["window_start", "window_end", "origin", "departures"],
        replace=True,
    )


q0 = (
    flights.writeStream.outputMode("append")
    .foreachBatch(write_flight_events)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/flight_events")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

q1 = (
    live_counts.writeStream.outputMode("complete")
    .foreachBatch(write_live_counts)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/live_counts")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

q2 = (
    airline_perf.writeStream.outputMode("complete")
    .foreachBatch(write_airline_perf)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/airline_perf")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

q3 = (
    airport_traffic.writeStream.outputMode("complete")
    .foreachBatch(write_airport_traffic)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/airport_traffic")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print("Spark Structured Streaming running")
print(f"  Topic       : {TOPIC}")
print(f"  Sink        : {SINK}")
print(f"  Output dir  : {OUTPUT_DIR}")
print(f"  Checkpoints : {CHECKPOINT_DIR}")
if SINK in {"postgres", "both"}:
    print(f"  Postgres    : {PG_HOST}:{PG_PORT}/{PG_DB}")

q0.awaitTermination()
