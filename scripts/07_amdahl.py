# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Script 07 — Amdahl's Law Benchmark Runner
# ============================================================================
#
# Executes the batch pipeline sequentially across local[1], local[2], and
# local[4] cores, measuring execution time differences to empirically plot
# parallel speedup limits using Amdahl's Law.
#
# Run with:
#     python scripts/07_amdahl.py
#
# ============================================================================

import os
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, max as spark_max, round as spark_round, when


APP_NAME = "TrafficWatchAmdahlBenchmark"
# P = fraction of work that can be parallelized (85%)
# The remaining 15% is sequential overhead (driver, scheduling, I/O)
PARALLEL_FRACTION = 0.85


def theoretical_speedup(cores: int, p: float = PARALLEL_FRACTION) -> float:
    """Amdahl's Law: S(N) = 1 / ((1 - P) + P/N)"""
    return 1.0 / ((1.0 - p) + p / cores)


def run_spark_job(core_count: int) -> float:
    """
    Run the full TrafficWatch batch analysis with a specific number of
    cores and return the wall-clock execution time in seconds.
    """
    project_root = Path(__file__).resolve().parents[1]
    input_path = str(project_root / "data" / "traffic_accidents.csv")
    tmp_dir = project_root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .master(f"local[{core_count}]")
        .appName(APP_NAME)
        .config("spark.local.dir", str(tmp_dir))
        .config("spark.driver.extraJavaOptions", f'-Djava.io.tmpdir="{tmp_dir}"')
        .config("spark.executor.extraJavaOptions", f'-Djava.io.tmpdir="{tmp_dir}"')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Force local filesystem scheme to avoid Hadoop HDFS path resolution errors
    input_path = "file://" + str(project_root / "data" / "traffic_accidents.csv")

    start_time = time.time()
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

        # Force materialization
        df.count()

        # Analysis 1: Traffic Class distribution
        df_classified = df.withColumn(
            "traffic_class",
            when(col("severity_score") < 3.0, lit("Minor Fender Bender"))
            .when((col("severity_score") >= 3.0) & (col("severity_score") < 5.0), lit("Moderate Collision"))
            .when((col("severity_score") >= 5.0) & (col("severity_score") < 7.0), lit("Severe Accident"))
            .otherwise(lit("Major Multi-Vehicle Pileup")),
        )
        df_classified.groupBy("traffic_class").agg(count("*").alias("count")).orderBy(col("count").desc()).collect()

        # Analysis 2: Regional distribution
        df.groupBy("region").agg(
            count("*").alias("total_crashes"),
            spark_round(avg("severity_score"), 2).alias("avg_severity"),
            spark_round(spark_max("severity_score"), 2).alias("max_severity"),
        ).collect()

        # Analysis 3: Top 10 dangerous streets
        df.groupBy("street_name").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10).collect()

        # Analysis 4: Pakistan stats
        df.filter(col("region") == "Pakistan").agg(
            avg("severity_score").alias("avg_severity"),
            spark_max("severity_score").alias("max_severity"),
        ).collect()

        # Analysis 5: Crash type breakdown
        df.groupBy("crash_type").agg(count("*").alias("count")).orderBy(col("count").desc()).collect()

        elapsed = time.time() - start_time
        return elapsed
    finally:
        spark.stop()


def print_results_table(cores_list, times, speedups):
    """Print a formatted results table."""
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70 + "\n")

    header = f"  {'Cores':<8} {'Time (s)':<12} {'Actual Speedup':<18} {'Theoretical':<12}"
    print(header)
    print("  " + "-" * 62)

    for cores in cores_list:
        t = times[cores]
        actual = speedups[cores]
        theory = theoretical_speedup(cores)
        print(f"  {cores:<8} {t:<12.2f} {actual:<18.2f} {theory:<12.2f}")

    print("  " + "-" * 62 + "\n")


def save_chart(output_file, times, speedups):
    """Create a chart comparing actual vs theoretical speedup."""
    measured_cores = [1, 2, 4]
    measured_speedups = [speedups[c] for c in measured_cores]

    all_cores = list(range(1, 33))
    theoretical_speedups = [theoretical_speedup(c) for c in all_cores]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left: Bar chart
    colors = ["#2563eb", "#f59e0b", "#10b981"]
    bars = ax1.bar([str(c) for c in measured_cores], measured_speedups,
                   color=colors, alpha=0.85, edgecolor="black")
    ax1.set_title("TrafficWatch — Actual Speedup", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Number of Cores", fontsize=11)
    ax1.set_ylabel("Speedup Factor", fontsize=11)
    ax1.grid(axis="y", linestyle="--", alpha=0.4)
    for bar, val in zip(bars, measured_speedups):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                 f"{val:.2f}", ha="center", va="bottom", fontsize=10)

    # Right: Line graph
    ax2.plot(all_cores, theoretical_speedups, "b-", linewidth=2.5,
             label=f"Theoretical (P={PARALLEL_FRACTION})", alpha=0.8)
    ax2.plot(measured_cores, measured_speedups, "ro-", linewidth=2,
             markersize=7, label="Actual Measured", alpha=0.8)
    ax2.set_title("Amdahl's Law: Theory vs Reality", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Number of Cores", fontsize=11)
    ax2.set_ylabel("Speedup Factor", fontsize=11)
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.legend(loc="lower right", fontsize=10)
    ax2.set_xlim(0, 33)

    fig.tight_layout()
    fig.savefig(output_file, dpi=200, bbox_inches="tight")
    print(f"  ✓ Chart saved to: {output_file}\n")
    plt.close(fig)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    tmp_dir = project_root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = str(tmp_dir)

    print("\n" + "=" * 70)
    print("  TrafficWatch — AMDAHL'S LAW BENCHMARK")
    print("=" * 70 + "\n")

    cores_to_test = [1, 2, 4]
    execution_times = {}

    for num_cores in cores_to_test:
        print(f"  Running with {num_cores} core(s)...", end=" ", flush=True)
        elapsed = run_spark_job(num_cores)
        execution_times[num_cores] = elapsed
        print(f"✓ {elapsed:.2f}s")

    baseline = execution_times[1]
    actual_speedups = {c: baseline / execution_times[c] for c in cores_to_test}

    output_dir = project_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_path = output_dir / "speedup_chart.png"

    save_chart(chart_path, execution_times, actual_speedups)
    print_results_table(cores_to_test, execution_times, actual_speedups)

    print("  Interpretation:")
    print(f"    • 2 cores: {actual_speedups[2]:.2f}x speedup (theory: {theoretical_speedup(2):.2f}x)")
    print(f"    • 4 cores: {actual_speedups[4]:.2f}x speedup (theory: {theoretical_speedup(4):.2f}x)")
    print(f"    • Gap shows parallelization overhead (scheduling, I/O, JVM)\n")


if __name__ == "__main__":
    main()
