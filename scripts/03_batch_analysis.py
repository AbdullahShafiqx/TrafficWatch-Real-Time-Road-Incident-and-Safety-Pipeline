# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Script 03 — PySpark Batch Analysis
# ============================================================================
#
# Loads traffic accident data from HDFS, applies when/otherwise classification
# of severity_score into Traffic Classes, groups by region to compute
# avg(severity_score), max(severity_score), and total crash counts.
# Saves outputs in parallel back to HDFS.
#
# Run with:
#     spark-submit scripts/03_batch_analysis.py
#
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    when,
)

# ---------------------------------------------------------------------------
# HDFS paths — uses the TrafficWatch directory structure
# ---------------------------------------------------------------------------

INPUT_PATH = "/traffic_watch/raw/traffic_accidents.csv"
OUTPUT_BASE = "/traffic_watch/output/batch"


# ---------------------------------------------------------------------------
# Traffic Class classification based on severity_score
# ---------------------------------------------------------------------------


def classify_traffic_class(df):
    """
    Map severity_score to clear Traffic Classes using when/otherwise:

      severity_score 1.0 – 3.0  →  Minor Fender Bender
      severity_score 3.0 – 5.0  →  Moderate Collision
      severity_score 5.0 – 7.0  →  Severe Accident
      severity_score 7.0 – 10.0 →  Major Multi-Vehicle Pileup
    """
    return df.withColumn(
        "traffic_class",
        when(col("severity_score") < 3.0, lit("Minor Fender Bender"))
        .when((col("severity_score") >= 3.0) & (col("severity_score") < 5.0), lit("Moderate Collision"))
        .when((col("severity_score") >= 5.0) & (col("severity_score") < 7.0), lit("Severe Accident"))
        .otherwise(lit("Major Multi-Vehicle Pileup")),
    )


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------


def analyze_traffic_class_distribution(df):
    """Distribution of incidents across Traffic Classes."""
    class_dist = (
        df.groupBy("traffic_class")
        .agg(
            count("*").alias("incident_count"),
            spark_round(avg("severity_score"), 2).alias("avg_severity"),
            spark_round(spark_max("severity_score"), 2).alias("max_severity"),
        )
        .orderBy(col("incident_count").desc())
    )

    print("\n  Traffic Class Distribution:")
    print("  " + "-" * 65)
    class_dist.show(truncate=False)
    return class_dist


def analyze_by_region(df):
    """
    Group by region and compute:
      - avg(severity_score)
      - max(severity_score)
      - total crash counts
    """
    region_stats = (
        df.groupBy("region")
        .agg(
            count("*").alias("total_crashes"),
            spark_round(avg("severity_score"), 2).alias("avg_severity_score"),
            spark_round(spark_max("severity_score"), 2).alias("max_severity_score"),
            spark_round(spark_min("severity_score"), 2).alias("min_severity_score"),
        )
        .orderBy(col("total_crashes").desc())
    )

    print("\n  Regional Crash Statistics:")
    print("  " + "-" * 65)
    region_stats.show(truncate=False)
    return region_stats


def analyze_top_streets(df):
    """Find the top 10 most dangerous streets/corridors."""
    top_streets = (
        df.groupBy("street_name")
        .agg(
            count("*").alias("incident_count"),
            spark_round(avg("severity_score"), 2).alias("avg_severity"),
        )
        .orderBy(col("incident_count").desc())
        .limit(10)
    )

    print("\n  Top 10 Most Dangerous Streets/Corridors:")
    print("  " + "-" * 65)
    top_streets.show(truncate=False)
    return top_streets


def analyze_crash_type_breakdown(df):
    """Breakdown of crash types (derived from TomTom iconCategory)."""
    crash_types = (
        df.groupBy("crash_type")
        .agg(
            count("*").alias("incident_count"),
            spark_round(avg("severity_score"), 2).alias("avg_severity"),
        )
        .orderBy(col("incident_count").desc())
    )

    print("\n  Crash Type Breakdown:")
    print("  " + "-" * 65)
    crash_types.show(truncate=False)
    return crash_types


def analyze_pakistan(df):
    """Detailed statistics for Pakistan region."""
    pakistan_df = df.filter(col("region") == "Pakistan")

    pakistan_stats = pakistan_df.agg(
        count("*").alias("total_incidents"),
        spark_round(avg("severity_score"), 2).alias("avg_severity_score"),
        spark_round(spark_max("severity_score"), 2).alias("max_severity_score"),
        spark_round(spark_min("severity_score"), 2).alias("min_severity_score"),
        spark_round(avg("speed_limit"), 0).alias("avg_speed_limit"),
    )

    print("\n  Pakistan Traffic Incident Statistics:")
    print("  " + "-" * 65)
    pakistan_stats.show(truncate=False)
    return pakistan_stats


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    spark = SparkSession.builder.appName("TrafficWatchBatchAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 70)
    print("  TrafficWatch — Batch Analysis Pipeline")
    print("=" * 70)

    # Load data from HDFS with the TrafficWatch unified schema
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    total_records = df.count()
    print(f"\n  Total traffic incident records loaded: {total_records:,}")

    if total_records == 0:
        print("\n  ⚠ No records found in HDFS. Run scripts 01 and 02 first.")
        spark.stop()
        return

    # Apply traffic class classification
    df = classify_traffic_class(df)

    # Run all analyses
    class_dist = analyze_traffic_class_distribution(df)
    region_stats = analyze_by_region(df)
    top_streets = analyze_top_streets(df)
    crash_types = analyze_crash_type_breakdown(df)
    pakistan_stats = analyze_pakistan(df)

    # Save all results in parallel back to HDFS
    print("\n  Saving results to HDFS...")

    class_dist.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/traffic_classes")
    region_stats.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/region_stats")
    top_streets.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/top_streets")
    crash_types.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/crash_types")
    pakistan_stats.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/pakistan_stats")

    print(f"  ✓ Traffic Classes      → {OUTPUT_BASE}/traffic_classes")
    print(f"  ✓ Region Statistics    → {OUTPUT_BASE}/region_stats")
    print(f"  ✓ Top Streets          → {OUTPUT_BASE}/top_streets")
    print(f"  ✓ Crash Types          → {OUTPUT_BASE}/crash_types")
    print(f"  ✓ Pakistan Statistics  → {OUTPUT_BASE}/pakistan_stats")
    print(f"\n  ✓ Batch analysis complete!\n")

    spark.stop()


if __name__ == "__main__":
    main()
