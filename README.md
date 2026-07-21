<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/Hadoop%20HDFS-3.4.x-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=white" alt="Hadoop"/>
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Flask-3.1.3-000000?style=for-the-badge&logo=flask&logoColor=white" alt="Flask"/>
  <img src="https://img.shields.io/badge/TomTom%20API-v5-FF6600?style=for-the-badge" alt="TomTom"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License"/>
</p>

<h1 align="center">🛡️ TrafficWatch</h1>
<h3 align="center">Real-Time Road Incident & Safety Pipeline</h3>

<p align="center">
  <em>An enterprise-grade distributed Big Data pipeline for road safety monitoring, incident classification, and parallel computing benchmarking — powered by Hadoop, Spark, and real-time TomTom telemetry.</em>
</p>

---

## 🌐 Overview

**TrafficWatch** is a full-stack distributed analytics platform that ingests live road incident data from the **TomTom Traffic API v5**, stores it in **Hadoop HDFS**, processes it through **PySpark batch and streaming engines**, and presents insights through a professional **Flask dashboard** with interactive maps, severity charts, and blackspot detection grids.

The pipeline covers two simultaneous geographic zones:

| Zone | Coverage | Purpose |
|------|----------|---------|
| 🌍 **Global Transit Corridor** | London Metro Area | Baseline high-density reference feed |
| 🇵🇰 **Pakistan Motorway Network** | M-2, GT Road, Lahore, Islamabad, Karachi | Targeted national road safety analysis |

> **Dual-Mode Engine** — The dashboard operates seamlessly whether Hadoop/HDFS is online (**HDFS Cluster Synced** mode) or offline (**Dynamic Local Fallback** mode), ensuring immediate interactivity under all configurations.

---

## ✨ Key Features

<table>
<tr>
<td width="50%">

### 🔄 Data Ingestion
- Real-time TomTom API v5 incident consumption
- Dual bounding-box queries (Global + Pakistan)
- Automatic fallback projection engine for Pakistan motorway grids when live data is sparse

</td>
<td width="50%">

### 💾 Distributed Storage
- Hadoop HDFS as the storage backbone
- Raw incident logs at `/traffic_watch/raw/`
- Analytical outputs at `/traffic_watch/output/`

</td>
</tr>
<tr>
<td width="50%">

### ⚡ Batch Processing
- PySpark SQL for parallelized grouping & classification
- Traffic Class mapping (Minor → Major Multi-Vehicle Pileup)
- Top 10 dangerous corridors, crash type breakdowns
- Regional statistics (Global vs Pakistan)

</td>
<td width="50%">

### 🔥 Blackspot Detection
- 0.1° grid aggregation (~10km × 10km cells)
- Density-based risk classification
- Labels: `CRITICAL BLACKSPOT` → `LOW RISK AREA`

</td>
</tr>
<tr>
<td width="50%">

### 📡 Real-Time Streaming
- TCP socket simulator (port 9999) for live telemetry feed
- PySpark Structured Streaming micro-batch consumer
- Critical alert flagging (severity > 7.0)
- Automatic HDFS Parquet archival

</td>
<td width="50%">

### 📊 Amdahl's Law Benchmark
- Empirical scaling experiments across 1, 2, and 4 CPU cores
- Theoretical vs actual speedup plotting (P = 0.85)
- Core efficiency analysis

</td>
</tr>
</table>

---

## 🖥️ Dashboard Preview

The TrafficWatch dashboard is a professional, single-page interface built with Flask, Leaflet.js, Chart.js, and Bootstrap 5.3.

<details>
<summary><b>📸 Click to expand Dashboard Screenshots</b></summary>

<br>

| Section | Preview |
|---------|---------|
| **About & Architecture** | ![About Section](docs/screenshots/pipeline-execution/17_dashboard_about_section.png) |
| **KPI Cards** | ![KPI Cards](docs/screenshots/pipeline-execution/18_dashboard_kpi_cards.png) |
| **Live Incident Map & Traffic Classes** | ![Map & Chart](docs/screenshots/pipeline-execution/19_dashboard_map_and_chart.png) |
| **Dangerous Corridors & Crash Types** | ![Corridors Tab](docs/screenshots/pipeline-execution/20_dashboard_corridors_tab.png) |
| **Accident Blackspots Grid** | ![Blackspots Tab](docs/screenshots/pipeline-execution/21_dashboard_blackspots_tab.png) |
| **Amdahl's Law Benchmark** | ![Amdahl Tab](docs/screenshots/pipeline-execution/22_dashboard_amdahl_tab.png) |
| **Core Scaling Matrix** | ![Scaling Matrix](docs/screenshots/pipeline-execution/23_dashboard_scaling_matrix.png) |

</details>

---

## 🏗️ Architecture

```
                    ┌──────────────────────────────┐
                    │    TomTom Traffic API v5      │
                    │   (Live Incident Telemetry)   │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │   01_download_data.py         │
                    │   API Ingestion + Fallback    │
                    │   Projection Engine           │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────▼────────────────────┐
              │          data/traffic_accidents.csv      │
              └────────────────────┬────────────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │   02_upload_hdfs.sh           │
                    │   HDFS Directory Setup &      │
                    │   CSV Upload                  │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────▼────────────────────┐
              │          Hadoop HDFS Cluster             │
              │   /traffic_watch/raw/                    │
              │   /traffic_watch/output/                 │
              └───┬─────────────┬──────────────┬────────┘
                  │             │              │
    ┌─────────────▼──┐  ┌──────▼──────┐  ┌───▼──────────────┐
    │ 03_batch_       │  │ 04_hotspot  │  │ 05_stream_feed   │
    │ analysis.py     │  │ .py         │  │ .py (TCP:9999)   │
    │                 │  │             │  │        │         │
    │ Traffic Classes │  │ Blackspot   │  │        ▼         │
    │ Region Stats    │  │ Detection   │  │ 06_stream_alert  │
    │ Top Streets     │  │ Risk Levels │  │ .py              │
    │ Crash Types     │  │             │  │ Critical Alerts  │
    └────────┬────────┘  └──────┬──────┘  └────────┬─────────┘
             │                  │                   │
             └──────────────────┼───────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   07_amdahl.py          │
                    │   Scaling Benchmark     │
                    │   (1, 2, 4 cores)       │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                    │       app.py            │
                    │   Flask REST API &      │
                    │   Interactive Dashboard │
                    │   (Dual-Mode Engine)    │
                    └────────────────────────┘
```

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|-----------|
| **Language** | Python 3.10+ |
| **Processing Engine** | Apache Spark 4.1.1 (PySpark SQL, Structured Streaming) |
| **Storage** | Hadoop HDFS 3.4.x |
| **Data Serialization** | CSV, Apache Parquet |
| **Backend** | Flask 3.1.3 |
| **Frontend** | HTML5, Vanilla CSS, Bootstrap 5.3, Bootstrap Icons |
| **Mapping** | Leaflet.js + Marker Clustering |
| **Charts** | Chart.js (Doughnut, Bar) |
| **API Provider** | TomTom Traffic API v5 |
| **Operating System** | Fedora Linux 43 |

---

## 📁 Repository Structure

```
TrafficWatch/
├── app.py                          # Flask backend — REST API & dashboard server
├── requirements.txt                # Runtime dependencies (Flask, Pandas, Requests)
├── requirements-pipeline.txt       # Pipeline dependencies (PySpark, Matplotlib)
│
├── scripts/                        # Modular pipeline scripts (sequential execution)
│   ├── 01_download_data.py         #   TomTom API ingestion + fallback projections
│   ├── 02_upload_hdfs.sh           #   Hadoop daemon check + HDFS CSV upload
│   ├── 03_batch_analysis.py        #   PySpark batch: classes, regions, streets, types
│   ├── 04_hotspot.py               #   PySpark blackspot grid aggregation
│   ├── 05_stream_feed.py           #   TCP socket simulator (live telemetry)
│   ├── 06_stream_alert.py          #   PySpark Structured Streaming alert consumer
│   └── 07_amdahl.py                #   Amdahl's Law benchmark (1/2/4 cores)
│
├── templates/
│   └── index.html                  # Single-page interactive dashboard
│
├── data/
│   └── traffic_accidents.csv       # Raw incident dataset (auto-generated)
│
├── output/
│   └── speedup_chart.png           # Amdahl benchmark visualization
│
└── docs/                           # Documentation & evidence
    ├── Traffic_Watch_Report.pdf     #   Formal project report
    ├── hadoop_spark_installation_manual.pdf
    └── screenshots/
        ├── hadoop-spark-setup/     #   Hadoop & Spark installation evidence
        └── pipeline-execution/     #   Full pipeline execution screenshots
```

---

## 🚀 Quick Start

### Prerequisites

| Requirement | Version |
|------------|---------|
| Python | 3.10+ |
| Java JDK | 17+ |
| Apache Hadoop | 3.4.x |
| Apache Spark | 4.1.x |
| TomTom API Key | [Free Developer Key](https://developer.tomtom.com/) |

### Phase 1 — Environment Setup

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-pipeline.txt

# Export your TomTom API key
export TOMTOM_KEY=your_actual_tomtom_api_key
```

### Phase 2 — Data Ingestion

```bash
# Download incidents from TomTom API (saves to data/traffic_accidents.csv)
python scripts/01_download_data.py
```

<details>
<summary>📸 Ingestion Output</summary>

![API Ingestion](docs/screenshots/pipeline-execution/01_api_ingestion_output.png)

</details>

### Phase 3 — Hadoop & Spark Processing

```bash
# Start Hadoop daemons
start-dfs.sh
start-yarn.sh

# Verify services
jps  # Should show: NameNode, DataNode, ResourceManager

# Upload data to HDFS
bash scripts/02_upload_hdfs.sh

# Run PySpark batch analysis
spark-submit scripts/03_batch_analysis.py

# Run blackspot detection
spark-submit scripts/04_hotspot.py
```

<details>
<summary>📸 Batch Processing Evidence</summary>

| Step | Screenshot |
|------|-----------|
| Hadoop Start | ![Hadoop](docs/screenshots/pipeline-execution/02_hadoop_daemons_start.png) |
| JPS Verification | ![JPS](docs/screenshots/pipeline-execution/03_jps_verification.png) |
| HDFS Upload | ![HDFS](docs/screenshots/pipeline-execution/04_hdfs_upload.png) |
| Batch Results | ![Batch](docs/screenshots/pipeline-execution/06_batch_class_region_stats.png) |
| Blackspots | ![Blackspots](docs/screenshots/pipeline-execution/10_global_blackspots_table.png) |

</details>

### Phase 4 — Real-Time Streaming

```bash
# Terminal 1: Start TCP stream simulator
source .venv/bin/activate
python scripts/05_stream_feed.py

# Terminal 2: Start Spark streaming consumer
source .venv/bin/activate
spark-submit scripts/06_stream_alert.py
```

> Events with `severity_score > 7.0` trigger **CRITICAL TRAFFIC INCIDENT** alerts and are archived to HDFS Parquet.

<details>
<summary>📸 Streaming Evidence</summary>

| Step | Screenshot |
|------|-----------|
| Feed Simulator | ![Feed](docs/screenshots/pipeline-execution/12_stream_feed_simulator.png) |
| Alert Batches | ![Alerts](docs/screenshots/pipeline-execution/13_stream_alert_batches.png) |

</details>

### Phase 5 — Performance Benchmark

```bash
# Run Amdahl's Law scaling benchmark (1, 2, 4 cores)
python scripts/07_amdahl.py
```

<details>
<summary>📸 Benchmark Results</summary>

![Amdahl Results](docs/screenshots/pipeline-execution/16_amdahl_results_summary.png)

</details>

### Phase 6 — Launch Dashboard

```bash
# Start the Flask server
python app.py
```

Navigate to **http://localhost:5001/** to view the interactive dashboard.

> [!NOTE]
> The Flask server includes a **Dual-Mode Data Engine**. If Hadoop/HDFS is active, it queries pre-processed HDFS files and displays the **HDFS Cluster Synced** badge. If HDFS is offline, the server automatically computes analytics from the local CSV and displays the **Dynamic Local Fallback** badge.

---

## 📊 Dashboard Sections

| Tab | Description |
|-----|-------------|
| 🗺️ **Live Incident Hotspots Map** | Interactive Leaflet map with severity-colored markers, cluster grouping, and a Doughnut chart showing traffic class distribution |
| 🛣️ **Dangerous Corridors & Crash Types** | Top 10 dangerous streets with severity progress bars + categorized crash type frequency bar chart |
| ⚠️ **Accident Blackspots** | Searchable grid-aggregated table (~10km cells) with density ranking, danger level badges, and one-click map focus navigation |
| 📈 **Parallel Scaling Benchmark** | Amdahl's Law speedup chart with a core performance scaling matrix showing theoretical vs measured speedup and efficiency percentages |

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [Traffic Watch Report](docs/Traffic_Watch_Report.pdf) | Comprehensive project report covering architecture, methodology, results, and analysis |
| [Hadoop & Spark Installation Manual](docs/hadoop_spark_installation_manual.pdf) | Step-by-step Fedora Linux setup guide for Hadoop HDFS & Apache Spark |

---

## 🔑 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Serves the interactive dashboard |
| `/api/summary` | GET | Regional statistics (Global vs Pakistan) |
| `/api/classes` | GET | Traffic class distribution |
| `/api/top_streets` | GET | Top 10 dangerous corridors |
| `/api/crash_types` | GET | Crash type breakdown |
| `/api/hotspots` | GET | Grid-aggregated accident blackspots |
| `/api/accidents` | GET | Raw coordinate data for Leaflet mapping |
| `/api/speedup` | GET | Serves the Amdahl speedup chart image |

---

## ⚙️ Hadoop & Spark Setup

For a complete environment setup guide on **Fedora Linux**, including Java JDK, Hadoop HDFS configuration, YARN, and PySpark installation, refer to:

📖 [**Hadoop & Spark Installation Manual**](docs/hadoop_spark_installation_manual.pdf)

<details>
<summary>📸 Setup Evidence Screenshots</summary>

| Step | Screenshot |
|------|-----------|
| SSH Setup | ![SSH](docs/screenshots/hadoop-spark-setup/ss_00_ssh_setup.png) |
| Java Version | ![Java](docs/screenshots/hadoop-spark-setup/ss_01_java_version.png) |
| Hadoop Version | ![Hadoop](docs/screenshots/hadoop-spark-setup/ss_02_hadoop_version.png) |
| NameNode Format | ![Format](docs/screenshots/hadoop-spark-setup/ss_03_fomat_namenode.png) |
| Start Hadoop | ![Start](docs/screenshots/hadoop-spark-setup/ss_04_start_hadoop.png) |
| JPS Processes | ![JPS](docs/screenshots/hadoop-spark-setup/ss_05_jps.png) |
| WordCount Test | ![WordCount](docs/screenshots/hadoop-spark-setup/ss_06_wordcount.png) |
| PySpark Shell | ![PySpark](docs/screenshots/hadoop-spark-setup/ss_07_pyspark_shell.png) |
| PySpark DataFrame | ![DataFrame](docs/screenshots/hadoop-spark-setup/ss_08_pyspark_dataframe.png) |
| NameNode UI | ![NameNode](docs/screenshots/hadoop-spark-setup/ss_09_namenode_ui.png) |
| YARN UI | ![YARN](docs/screenshots/hadoop-spark-setup/ss_10_yarn_ui.png) |
| Spark UI | ![Spark](docs/screenshots/hadoop-spark-setup/ss_11_spark_ui.png) |

</details>

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  <img src="https://img.shields.io/badge/Made%20with-❤️-red?style=for-the-badge" alt="Made with Love"/>
</p>

<h3 align="center">Created with ❤️ by Abdullah Shafiq</h3>

<p align="center">
  <a href="https://github.com/AbdullahShafiqx">
    <img src="https://img.shields.io/badge/GitHub-AbdullahShafiqx-181717?style=for-the-badge&logo=github&logoColor=white" alt="GitHub"/>
  </a>
</p>
