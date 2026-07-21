# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Flask Backend — REST API & Dashboard Server
# ============================================================================

import csv
import io
import os
import subprocess
import sys
from pathlib import Path
from collections import defaultdict

from flask import Flask, jsonify, render_template, send_file

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
APP_NAME = "TrafficWatch"

app = Flask(__name__)

# HDFS Paths
HDFS_RAW_CSV = "/traffic_watch/raw/traffic_accidents.csv"
HDFS_REGION_STATS = "/traffic_watch/output/batch/region_stats/*.csv"
HDFS_TRAFFIC_CLASSES = "/traffic_watch/output/batch/traffic_classes/*.csv"
HDFS_TOP_STREETS = "/traffic_watch/output/batch/top_streets/*.csv"
HDFS_CRASH_TYPES = "/traffic_watch/output/batch/crash_types/*.csv"
HDFS_HOTSPOTS_GLOBAL = "/traffic_watch/output/hotspots/global/*.csv"
HDFS_HOTSPOTS_PAKISTAN = "/traffic_watch/output/hotspots/pakistan/*.csv"

# ---------------------------------------------------------------------------
# HDFS Helpers
# ---------------------------------------------------------------------------

def read_hdfs_csv(hdfs_path: str) -> list[dict]:
    """
    Reads a CSV from HDFS using the Hadoop CLI and parses it into a list of dicts.
    Handles wildcard paths (e.g., part-00000.csv).
    """
    try:
        completed = subprocess.run(
            ["hdfs", "dfs", "-cat", hdfs_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        output = completed.stdout.strip()
        if not output:
            return []
            
        lines = output.splitlines()
        if not lines:
            return []
            
        header = lines[0]
        clean_lines = [header] + [line for line in lines[1:] if line != header and line.strip()]
        
        reader = csv.DictReader(clean_lines)
        return list(reader)
        
    except subprocess.CalledProcessError as e:
        print(f"HDFS Read Error for {hdfs_path}: {e.stderr}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error parsing HDFS CSV {hdfs_path}: {str(e)}", file=sys.stderr)
        return []

# ---------------------------------------------------------------------------
# Local Directory Fallback Reader
# ---------------------------------------------------------------------------

def read_local_part_csvs(dir_name: str) -> list[dict]:
    """
    Reads all part-*.csv files inside a local output folder and parses them.
    Used as a secondary fallback if HDFS is down but local output exists.
    """
    for sub in ["batch", "hotspots"]:
        local_dir = BASE_DIR / "output" / sub / dir_name
        if local_dir.exists():
            csv_files = list(local_dir.glob("*.csv"))
            if csv_files:
                combined_rows = []
                header = None
                for file_path in csv_files:
                    try:
                        with open(file_path, mode="r", newline="", encoding="utf-8") as f:
                            content = f.read().strip()
                            if not content:
                                continue
                            lines = content.splitlines()
                            if not lines:
                                continue
                            if header is None:
                                header = lines[0]
                            clean_lines = [header] + [line for line in lines[1:] if line != header and line.strip()]
                            reader = csv.DictReader(clean_lines)
                            combined_rows.extend(list(reader))
                    except Exception as e:
                        print(f"Error reading local part CSV {file_path}: {e}", file=sys.stderr)
                if combined_rows:
                    return combined_rows
    return []

# ---------------------------------------------------------------------------
# Dynamic Computation Fallbacks (from data/traffic_accidents.csv)
# ---------------------------------------------------------------------------

def load_local_raw_accidents() -> list[dict]:
    """Load the raw accidents dataset from the local CSV."""
    local_path = BASE_DIR / "data" / "traffic_accidents.csv"
    if not local_path.exists():
        return []
    try:
        with open(local_path, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"Error reading local raw CSV: {e}", file=sys.stderr)
        return []

def get_alert_level(severity: float) -> str:
    """Classify severity_score into alert tiers."""
    if severity is None:
        return "LOW"
    if severity > 7.0:
        return "CRITICAL"
    if severity > 5.0:
        return "HIGH"
    if severity > 3.0:
        return "MEDIUM"
    return "LOW"

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def serve_dashboard():
    """Serve the TrafficWatch dashboard."""
    return render_template("index.html")

@app.route("/api/summary")
def api_summary():
    """
    Returns total counts, average severity, and maximum severity 
    split explicitly by 'Global' and 'Pakistan'.
    """
    # 1. Try HDFS
    rows = read_hdfs_csv(HDFS_REGION_STATS)
    
    # 2. Try Local Output Dir
    if not rows:
        rows = read_local_part_csvs("region_stats")
        
    # 3. Dynamic Fallback
    if not rows:
        print("API Summary: Falling back to dynamic local CSV computation", file=sys.stderr)
        accidents = load_local_raw_accidents()
        if not accidents:
            return jsonify({"global": {}, "pakistan": {}, "total_combined": 0})
            
        global_total = 0
        global_sev_sum = 0.0
        global_max_sev = 0.0
        pak_total = 0
        pak_sev_sum = 0.0
        pak_max_sev = 0.0
        
        for r in accidents:
            try:
                sev = float(r.get("severity_score", 0.0))
            except (ValueError, TypeError):
                continue
            region = r.get("region", "Global")
            if region == "Pakistan":
                pak_total += 1
                pak_sev_sum += sev
                if sev > pak_max_sev:
                    pak_max_sev = sev
            else:
                global_total += 1
                global_sev_sum += sev
                if sev > global_max_sev:
                    global_max_sev = sev
                    
        global_avg = (global_sev_sum / global_total) if global_total > 0 else 0.0
        pak_avg = (pak_sev_sum / pak_total) if pak_total > 0 else 0.0
        
        return jsonify({
            "app_name": APP_NAME,
            "global": {"total": global_total, "avg_severity": round(global_avg, 2), "max_severity": round(global_max_sev, 2)},
            "pakistan": {"total": pak_total, "avg_severity": round(pak_avg, 2), "max_severity": round(pak_max_sev, 2)},
            "total_combined": global_total + pak_total,
            "source": "dynamic_fallback"
        })
        
    # Standard Parse (from HDFS/Local Part file structure)
    global_stats = {"total": 0, "avg_severity": 0.0, "max_severity": 0.0}
    pak_stats = {"total": 0, "avg_severity": 0.0, "max_severity": 0.0}
    
    for r in rows:
        region = r.get("region", "Global")
        try:
            total = int(r.get("total_crashes", 0))
            avg_sev = float(r.get("avg_severity_score", 0.0))
            max_sev = float(r.get("max_severity_score", 0.0))
        except ValueError:
            continue
            
        if region == "Pakistan":
            pak_stats = {"total": total, "avg_severity": round(avg_sev, 2), "max_severity": round(max_sev, 2)}
        else:
            global_stats = {"total": total, "avg_severity": round(avg_sev, 2), "max_severity": round(max_sev, 2)}
            
    return jsonify({
        "app_name": APP_NAME,
        "global": global_stats,
        "pakistan": pak_stats,
        "total_combined": global_stats["total"] + pak_stats["total"],
        "source": "batch_output"
    })

@app.route("/api/classes")
def api_classes():
    """
    Returns the exact counting breakdown of Traffic Classes.
    """
    # 1. Try HDFS
    rows = read_hdfs_csv(HDFS_TRAFFIC_CLASSES)
    
    # 2. Try Local Output Dir
    if not rows:
        rows = read_local_part_csvs("traffic_classes")
        
    # 3. Dynamic Fallback
    if not rows:
        print("API Classes: Falling back to dynamic local CSV computation", file=sys.stderr)
        accidents = load_local_raw_accidents()
        class_counts = defaultdict(int)
        for r in accidents:
            try:
                sev = float(r.get("severity_score", 1.0))
            except (ValueError, TypeError):
                continue
            if sev < 3.0:
                cls_name = "Minor Fender Bender"
            elif sev < 5.0:
                cls_name = "Moderate Collision"
            elif sev < 7.0:
                cls_name = "Severe Accident"
            else:
                cls_name = "Major Multi-Vehicle Pileup"
            class_counts[cls_name] += 1
        return jsonify({"classes": dict(class_counts), "source": "dynamic_fallback"})
        
    classes_data = {}
    for r in rows:
        cls_name = r.get("traffic_class")
        try:
            count = int(r.get("incident_count", 0))
            classes_data[cls_name] = count
        except ValueError:
            continue
            
    return jsonify({"classes": classes_data, "source": "batch_output"})

@app.route("/api/top_streets")
def api_top_streets():
    """
    Returns the top 10 dangerous streets / incident corridors.
    """
    # 1. Try HDFS
    rows = read_hdfs_csv(HDFS_TOP_STREETS)
    
    # 2. Try Local Output Dir
    if not rows:
        rows = read_local_part_csvs("top_streets")
        
    # 3. Dynamic Fallback
    if not rows:
        print("API Top Streets: Falling back to dynamic local CSV computation", file=sys.stderr)
        accidents = load_local_raw_accidents()
        street_counts = defaultdict(int)
        street_sev_sum = defaultdict(float)
        for r in accidents:
            street = r.get("street_name", "Unknown Road")
            try:
                sev = float(r.get("severity_score", 0.0))
            except (ValueError, TypeError):
                continue
            street_counts[street] += 1
            street_sev_sum[street] += sev
            
        top_streets = []
        for street, count in street_counts.items():
            avg_sev = street_sev_sum[street] / count
            top_streets.append({
                "street_name": street,
                "incident_count": count,
                "avg_severity": round(avg_sev, 2)
            })
        top_streets.sort(key=lambda x: (x["incident_count"], x["avg_severity"]), reverse=True)
        return jsonify({"streets": top_streets[:10], "source": "dynamic_fallback"})
        
    streets_data = []
    for r in rows:
        try:
            streets_data.append({
                "street_name": r.get("street_name", "Unknown Road"),
                "incident_count": int(r.get("incident_count", 0)),
                "avg_severity": round(float(r.get("avg_severity", 0.0)), 2)
            })
        except ValueError:
            continue
    streets_data.sort(key=lambda x: x["incident_count"], reverse=True)
    return jsonify({"streets": streets_data[:10], "source": "batch_output"})

@app.route("/api/crash_types")
def api_crash_types():
    """
    Returns breakdown of incidents by crash type.
    """
    # 1. Try HDFS
    rows = read_hdfs_csv(HDFS_CRASH_TYPES)
    
    # 2. Try Local Output Dir
    if not rows:
        rows = read_local_part_csvs("crash_types")
        
    # 3. Dynamic Fallback
    if not rows:
        print("API Crash Types: Falling back to dynamic local CSV computation", file=sys.stderr)
        accidents = load_local_raw_accidents()
        type_counts = defaultdict(int)
        type_sev_sum = defaultdict(float)
        for r in accidents:
            c_type = r.get("crash_type", "Unknown Incident")
            try:
                sev = float(r.get("severity_score", 0.0))
            except (ValueError, TypeError):
                continue
            type_counts[c_type] += 1
            type_sev_sum[c_type] += sev
            
        types_list = []
        for c_type, count in type_counts.items():
            avg_sev = type_sev_sum[c_type] / count
            types_list.append({
                "crash_type": c_type,
                "incident_count": count,
                "avg_severity": round(avg_sev, 2)
            })
        types_list.sort(key=lambda x: x["incident_count"], reverse=True)
        return jsonify({"types": types_list, "source": "dynamic_fallback"})
        
    types_data = []
    for r in rows:
        try:
            types_data.append({
                "crash_type": r.get("crash_type", "Unknown Incident"),
                "incident_count": int(r.get("incident_count", 0)),
                "avg_severity": round(float(r.get("avg_severity", 0.0)), 2)
            })
        except ValueError:
            continue
    types_data.sort(key=lambda x: x["incident_count"], reverse=True)
    return jsonify({"types": types_data, "source": "batch_output"})

@app.route("/api/hotspots")
def api_hotspots():
    """
    Returns grid-aggregated accident hotspots for both global and Pakistan.
    """
    # 1. Try HDFS
    rows_glob = read_hdfs_csv(HDFS_HOTSPOTS_GLOBAL)
    rows_pak = read_hdfs_csv(HDFS_HOTSPOTS_PAKISTAN)
    rows = rows_glob + rows_pak
    
    # 2. Try Local Output Dir
    if not rows:
        rows_glob = read_local_part_csvs("global")
        rows_pak = read_local_part_csvs("pakistan")
        rows = rows_glob + rows_pak
        
    # 3. Dynamic Fallback
    if not rows:
        print("API Hotspots: Falling back to dynamic local CSV computation", file=sys.stderr)
        accidents = load_local_raw_accidents()
        grid_data = defaultdict(lambda: {"count": 0, "sev_sum": 0.0, "max_sev": 0.0})
        
        for r in accidents:
            try:
                lat = round(float(r.get("latitude")), 1)
                lon = round(float(r.get("longitude")), 1)
                sev = float(r.get("severity_score", 0.0))
            except (ValueError, TypeError):
                continue
            key = (lat, lon)
            grid_data[key]["count"] += 1
            grid_data[key]["sev_sum"] += sev
            if sev > grid_data[key]["max_sev"]:
                grid_data[key]["max_sev"] = sev
                
        hotspots = []
        for (lat, lon), stats in grid_data.items():
            count = stats["count"]
            avg_sev = stats["sev_sum"] / count
            max_sev = stats["max_sev"]
            
            if count > 50:
                danger = "CRITICAL BLACKSPOT"
            elif count > 30:
                danger = "HIGH DANGER CORRIDOR"
            elif count > 10:
                danger = "MODERATE RISK ZONE"
            else:
                danger = "LOW RISK AREA"
                
            hotspots.append({
                "lat_grid": lat,
                "lon_grid": lon,
                "incident_count": count,
                "avg_severity": round(avg_sev, 2),
                "max_severity": round(max_sev, 2),
                "danger_level": danger
            })
        hotspots.sort(key=lambda x: x["incident_count"], reverse=True)
        return jsonify({"hotspots": hotspots[:25], "source": "dynamic_fallback"})
        
    hotspots_data = []
    for r in rows:
        try:
            hotspots_data.append({
                "lat_grid": round(float(r.get("lat_grid")), 1),
                "lon_grid": round(float(r.get("lon_grid")), 1),
                "incident_count": int(r.get("incident_count", 0)),
                "avg_severity": round(float(r.get("avg_severity", 0.0)), 2),
                "max_severity": round(float(r.get("max_severity", 0.0)), 2),
                "danger_level": r.get("danger_level", "LOW RISK AREA")
            })
        except ValueError:
            continue
    hotspots_data.sort(key=lambda x: x["incident_count"], reverse=True)
    return jsonify({"hotspots": hotspots_data[:25], "source": "batch_output"})

@app.route("/api/accidents")
def api_accidents():
    """
    Returns raw coordinate array for Leaflet mapping.
    """
    # 1. Try HDFS
    rows = read_hdfs_csv(HDFS_RAW_CSV)
    
    # 2. Dynamic Fallback
    if not rows:
        print("API Accidents: Falling back to local CSV", file=sys.stderr)
        rows = load_local_raw_accidents()
        
    accidents = []
    for r in rows:
        try:
            severity = float(r.get("severity_score", 1.0))
            accidents.append({
                "latitude": float(r.get("latitude")),
                "longitude": float(r.get("longitude")),
                "street_name": r.get("street_name", "Unknown Road"),
                "crash_type": r.get("crash_type", "Unknown Incident"),
                "severity_score": severity,
                "region": r.get("region", "Global"),
                "alert_level": get_alert_level(severity)
            })
        except (ValueError, TypeError):
            continue
            
    return jsonify({"accidents": accidents})

@app.route("/api/speedup")
def get_speedup_chart():
    """Serves the generated speedup chart image."""
    chart_path = BASE_DIR / "output" / "speedup_chart.png"
    if not chart_path.exists():
        chart_path = BASE_DIR / "docs" / "screenshots" / "speedup_chart.png"
    if not chart_path.exists():
        return "Chart not found", 404
    return send_file(chart_path, mimetype="image/png")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
