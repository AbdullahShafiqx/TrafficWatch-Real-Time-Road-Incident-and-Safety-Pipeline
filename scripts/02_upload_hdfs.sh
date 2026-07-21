#!/usr/bin/env bash
# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Script 02 — Upload Traffic Accident CSV to HDFS
# ============================================================================
#
# Checks that Hadoop daemons (NameNode, DataNode) are alive, creates the
# /traffic_watch/raw/ directory structure in HDFS, and forcefully uploads
# data/traffic_accidents.csv, overwriting any previous copy.
#
# Usage:
#     bash scripts/02_upload_hdfs.sh
#
# ============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOCAL_FILE="data/traffic_accidents.csv"
HDFS_RAW_DIR="/traffic_watch/raw"
HDFS_OUTPUT_DIR="/traffic_watch/output"
HDFS_FILE_PATH="${HDFS_RAW_DIR}/traffic_accidents.csv"

# ---------------------------------------------------------------------------
# Step 1: Verify Hadoop daemons are running
# ---------------------------------------------------------------------------

echo ""
echo "========================================"
echo "  TrafficWatch — HDFS Upload"
echo "========================================"
echo ""

echo "[1/4] Checking if Hadoop daemons are alive..."

if ! command -v jps &>/dev/null; then
    echo "✗ 'jps' command not found."
    echo "  Make sure Java JDK is installed and JAVA_HOME is set."
    echo "  On Fedora:  sudo dnf install java-17-openjdk-devel"
    exit 1
fi

JPS_OUTPUT=$(jps 2>/dev/null || true)

if echo "${JPS_OUTPUT}" | grep -q "NameNode"; then
    echo "  ✓ NameNode is running"
else
    echo "  ✗ NameNode is NOT running."
    echo "    Start Hadoop with:  start-dfs.sh"
    exit 1
fi

if echo "${JPS_OUTPUT}" | grep -q "DataNode"; then
    echo "  ✓ DataNode is running"
else
    echo "  ⚠ DataNode not detected (pseudo-distributed mode may still work)"
fi

if echo "${JPS_OUTPUT}" | grep -q "ResourceManager"; then
    echo "  ✓ ResourceManager (YARN) is running"
else
    echo "  ⚠ ResourceManager not detected — YARN jobs may not work"
    echo "    Start YARN with:  start-yarn.sh"
fi

# ---------------------------------------------------------------------------
# Step 2: Check that the local CSV file exists
# ---------------------------------------------------------------------------

echo ""
echo "[2/4] Checking local CSV file..."

if [[ ! -f "${LOCAL_FILE}" ]]; then
    echo "  ✗ File not found: ${LOCAL_FILE}"
    echo "    Run 01_download_data.py first to fetch traffic incidents."
    exit 1
fi

FILE_SIZE=$(du -h "${LOCAL_FILE}" | cut -f1)
LINE_COUNT=$(wc -l < "${LOCAL_FILE}")
echo "  ✓ Found: ${LOCAL_FILE}"
echo "    Size: ${FILE_SIZE} | Lines: ${LINE_COUNT}"

# ---------------------------------------------------------------------------
# Step 3: Create HDFS directory structure
# ---------------------------------------------------------------------------

echo ""
echo "[3/4] Creating HDFS directory structure..."

hdfs dfs -mkdir -p "${HDFS_RAW_DIR}"
hdfs dfs -mkdir -p "${HDFS_OUTPUT_DIR}"
hdfs dfs -mkdir -p "${HDFS_OUTPUT_DIR}/batch"
hdfs dfs -mkdir -p "${HDFS_OUTPUT_DIR}/hotspots"
hdfs dfs -mkdir -p "${HDFS_OUTPUT_DIR}/streaming"

echo "  ✓ Created ${HDFS_RAW_DIR}"
echo "  ✓ Created ${HDFS_OUTPUT_DIR}/batch"
echo "  ✓ Created ${HDFS_OUTPUT_DIR}/hotspots"
echo "  ✓ Created ${HDFS_OUTPUT_DIR}/streaming"

# ---------------------------------------------------------------------------
# Step 4: Upload CSV to HDFS (force overwrite)
# ---------------------------------------------------------------------------

echo ""
echo "[4/4] Uploading to HDFS (overwriting previous copy)..."

hdfs dfs -put -f "${LOCAL_FILE}" "${HDFS_FILE_PATH}"

echo "  ✓ Upload complete: ${HDFS_FILE_PATH}"

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

echo ""
echo "Verifying upload..."
hdfs dfs -ls "${HDFS_RAW_DIR}"
echo ""
hdfs dfs -du -h "${HDFS_RAW_DIR}"

echo ""
echo "✓ TrafficWatch HDFS upload complete!"
echo "  Raw data path: ${HDFS_FILE_PATH}"
echo ""
