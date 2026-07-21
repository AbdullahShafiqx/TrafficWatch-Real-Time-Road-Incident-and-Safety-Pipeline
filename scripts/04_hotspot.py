# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Script 04 — Accident Blackspot & Dangerous Corridor Detection
# ============================================================================
#
# Rounds coordinates (round(col("latitude"), 1)) to aggregate crashes inside
# ~10km × 10km grid cells. Ranks the top high-density "Accident Blackspots
# and Dangerous Traffic Corridors" for both the Global baseline and
# Pakistan Motorways.
#
# Run with:
#     spark-submit scripts/04_hotspot.py
#
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    round as spark_round,
    when,
)

# ---------------------------------------------------------------------------
# HDFS paths
# ---------------------------------------------------------------------------

INPUT_PATH = "/traffic_watch/raw/traffic_accidents.csv"
GLOBAL_OUTPUT_PATH = "/traffic_watch/output/hotspots/global"
PAKISTAN_OUTPUT_PATH = "/traffic_watch/output/hotspots/pakistan"


# ---------------------------------------------------------------------------
# Risk classification
# ---------------------------------------------------------------------------


def add_danger_level(hotspots_df):
    """
    Classify grid cells by incident density:

      > 50 incidents  →  CRITICAL BLACKSPOT
      > 30 incidents  →  HIGH DANGER CORRIDOR
      > 10 incidents  →  MODERATE RISK ZONE
      ≤ 10 incidents  →  LOW RISK AREA
    """
    return hotspots_df.withColumn(
        "danger_level",
        when(col("incident_count") > 50, "CRITICAL BLACKSPOT")
        .when(col("incident_count") > 30, "HIGH DANGER CORRIDOR")
        .when(col("incident_count") > 10, "MODERATE RISK ZONE")
        .otherwise("LOW RISK AREA"),
    )


# ---------------------------------------------------------------------------
# Grid aggregation
# ---------------------------------------------------------------------------


def create_accident_grid(df, region_filter=None):
    """
    Round lat/lon to 1 decimal place (~10km precision) and aggregate
    crash metrics per grid cell.

    Parameters
    ----------
    df : DataFrame
        Input traffic accident DataFrame.
    region_filter : str or None
        If provided, filter to this region ('Pakistan' or 'Global').

    Returns
    -------
    DataFrame
        Grid-aggregated hotspot DataFrame ranked by incident count.
    """
    # Create grid cells by rounding coordinates
    grid_df = (
        df.withColumn("lat_grid", spark_round(col("latitude"), 1))
        .withColumn("lon_grid", spark_round(col("longitude"), 1))
    )

    if region_filter:
        grid_df = grid_df.filter(col("region") == region_filter)

    # Aggregate crash metrics per grid cell
    hotspots = (
        grid_df.groupBy("lat_grid", "lon_grid")
        .agg(
            count("*").alias("incident_count"),
            spark_round(avg("severity_score"), 2).alias("avg_severity"),
            spark_round(spark_max("severity_score"), 2).alias("max_severity"),
        )
        .orderBy(col("incident_count").desc())
    )

    return add_danger_level(hotspots)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    spark = SparkSession.builder.appName("TrafficWatchBlackspotDetection").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 70)
    print("  TrafficWatch — Accident Blackspot & Dangerous Corridor Detection")
    print("=" * 70)

    # Load traffic accident data from HDFS
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    total = df.count()
    print(f"\n  Loaded {total:,} traffic incident records")

    if total == 0:
        print("\n  ⚠ No records found. Run scripts 01 and 02 first.")
        spark.stop()
        return

    # ----- Global Accident Blackspots -----
    print("\n  Identifying Global Accident Blackspots (grid: 0.1° ≈ 10km)...")
    global_hotspots = create_accident_grid(df)

    print("\n  ╔══════════════════════════════════════════════════════════╗")
    print("  ║  TOP 15 GLOBAL ACCIDENT BLACKSPOTS                     ║")
    print("  ╚══════════════════════════════════════════════════════════╝")
    global_hotspots.select(
        "lat_grid", "lon_grid", "incident_count",
        "avg_severity", "max_severity", "danger_level"
    ).limit(15).show(truncate=False)

    # ----- Pakistan Motorway Blackspots -----
    print("  Identifying Pakistan Motorway Accident Blackspots...")
    pakistan_hotspots = create_accident_grid(df, region_filter="Pakistan")

    pak_count = pakistan_hotspots.count()
    print(f"  Found {pak_count} distinct grid cells in Pakistan\n")

    print("  ╔══════════════════════════════════════════════════════════╗")
    print("  ║  TOP 10 PAKISTAN DANGEROUS TRAFFIC CORRIDORS            ║")
    print("  ╚══════════════════════════════════════════════════════════╝")
    pakistan_hotspots.select(
        "lat_grid", "lon_grid", "incident_count",
        "avg_severity", "max_severity", "danger_level"
    ).limit(10).show(truncate=False)

    # ----- Save to HDFS -----
    print("  Saving blackspot analysis to HDFS...")

    global_hotspots.write.mode("overwrite").option("header", True).csv(GLOBAL_OUTPUT_PATH)
    pakistan_hotspots.write.mode("overwrite").option("header", True).csv(PAKISTAN_OUTPUT_PATH)

    print(f"  ✓ Global Blackspots  → {GLOBAL_OUTPUT_PATH}")
    print(f"  ✓ Pakistan Corridors → {PAKISTAN_OUTPUT_PATH}")
    print(f"\n  ✓ Blackspot detection complete!\n")

    spark.stop()


if __name__ == "__main__":
    main()
