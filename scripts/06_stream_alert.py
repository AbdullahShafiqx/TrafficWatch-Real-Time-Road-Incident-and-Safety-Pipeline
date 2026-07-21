# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Script 06 — PySpark Structured Streaming Alert System
# ============================================================================
#
# Listens to TCP port 9999, parses the incoming pipe-delimited stream
# against the TrafficWatch schema in micro-batches, and flags any event
# where severity_score > 7.0 with a prominent console alert:
#   "CRITICAL TRAFFIC INCIDENT !!"
#
# Critical records are automatically appended to HDFS Parquet.
#
# Usage:
#     # Terminal 1:  python scripts/05_stream_feed.py
#     # Terminal 2:  spark-submit scripts/06_stream_alert.py
#
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, when


SOCKET_HOST = "localhost"
SOCKET_PORT = 9999
ALERT_OUTPUT_PATH = "/traffic_watch/output/streaming/critical_alerts"
CHECKPOINT_PATH = "/traffic_watch/output/streaming/checkpoint"


def parse_stream_data(raw_stream):
    """
    Parse pipe-delimited records from the TCP socket into structured columns
    matching the TrafficWatch unified schema.

    Stream format (pipe-delimited):
      timestamp|latitude|longitude|speed_limit|severity_score|street_name|crash_type|region
    """
    parts = split(col("value"), "\\|")

    return (
        raw_stream
        .withColumn("timestamp", trim(parts.getItem(0)))
        .withColumn("latitude", trim(parts.getItem(1)).cast("double"))
        .withColumn("longitude", trim(parts.getItem(2)).cast("double"))
        .withColumn("speed_limit", trim(parts.getItem(3)).cast("integer"))
        .withColumn("severity_score", trim(parts.getItem(4)).cast("double"))
        .withColumn("street_name", trim(parts.getItem(5)))
        .withColumn("crash_type", trim(parts.getItem(6)))
        .withColumn("region", trim(parts.getItem(7)))
        .drop("value")
        .filter(col("timestamp").isNotNull() & col("severity_score").isNotNull())
    )


def classify_alert_level(stream_df):
    """
    Classify traffic incidents by severity_score into alert tiers:

      severity_score > 7.0  →  CRITICAL TRAFFIC INCIDENT !!
      severity_score > 5.0  →  HIGH
      severity_score > 3.0  →  MEDIUM
      severity_score ≤ 3.0  →  LOW
    """
    return (
        stream_df
        .withColumn(
            "alert_level",
            when(col("severity_score") > 7.0, "CRITICAL TRAFFIC INCIDENT !!")
            .when(col("severity_score") > 5.0, "HIGH")
            .when(col("severity_score") > 3.0, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("is_pakistan", col("region") == "Pakistan")
    )


def main() -> None:
    spark = SparkSession.builder.appName("TrafficWatchStreamingAlert").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 70)
    print("  TrafficWatch — Real-Time Streaming Alert System")
    print("=" * 70)
    print("\n  Listening for traffic incident data on socket...")
    print(f"  Host: {SOCKET_HOST}:{SOCKET_PORT}")
    print(f"  Critical threshold: severity_score > 7.0")
    print(f"  Parquet output: {ALERT_OUTPUT_PATH}\n")

    # Connect to the TCP socket stream
    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", SOCKET_HOST)
        .option("port", SOCKET_PORT)
        .load()
    )

    # Parse and classify
    parsed_stream = parse_stream_data(raw_stream)
    alerts = classify_alert_level(parsed_stream)

    # Console sink: show HIGH and CRITICAL alerts for monitoring
    high_critical = alerts.filter(
        (col("alert_level") == "HIGH")
        | (col("alert_level") == "CRITICAL TRAFFIC INCIDENT !!")
    ).select(
        "timestamp", "street_name", "severity_score",
        "crash_type", "alert_level", "region"
    )

    console_query = (
        high_critical.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    # Parquet sink: persist CRITICAL alerts to HDFS
    critical_alerts = alerts.filter(
        col("alert_level") == "CRITICAL TRAFFIC INCIDENT !!"
    ).select(
        "timestamp", "latitude", "longitude", "speed_limit",
        "severity_score", "street_name", "crash_type", "region",
        "alert_level", "is_pakistan"
    )

    parquet_query = (
        critical_alerts.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ALERT_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    # Keep streaming until interrupted
    spark.streams.awaitAnyTermination()
    console_query.stop()
    parquet_query.stop()


if __name__ == "__main__":
    main()
