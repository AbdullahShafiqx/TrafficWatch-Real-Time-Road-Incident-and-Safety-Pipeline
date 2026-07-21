# ============================================================================
# TrafficWatch: Real-Time Road Incident & Safety Pipeline
# Script 05 — TCP Socket Stream Simulator (Live Telemetry Feed)
# ============================================================================
#
# Opens data/traffic_accidents.csv and continuously streams records over
# TCP port 9999 line-by-line every 0.5 seconds to simulate an incoming
# live traffic telemetry feed.
#
# Usage:
#     python scripts/05_stream_feed.py
#
# ============================================================================

import csv
import socket
import time
from pathlib import Path

HOST = "localhost"
PORT = 9999
DELAY_SECONDS = 0.5

STREAM_COLUMNS = [
    "timestamp", "latitude", "longitude", "speed_limit",
    "severity_score", "street_name", "crash_type", "region",
]


def format_row_for_stream(row: dict) -> str:
    """Convert a CSV row dict to a pipe-delimited string for streaming."""
    values = []
    for key in STREAM_COLUMNS:
        value = row.get(key, "")
        if value is None:
            value = ""
        text = str(value).replace("\n", " ").replace("\r", " ").strip()
        values.append(text)
    return "|".join(values)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    data_file = project_root / "data" / "traffic_accidents.csv"

    if not data_file.exists():
        print(f"✗ CSV file not found: {data_file}")
        print("  Run 01_download_data.py first.")
        return

    with open(data_file, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("✗ CSV file is empty.")
        return

    print("=" * 60)
    print("  TrafficWatch — TCP Stream Feed Simulator")
    print("=" * 60)
    print(f"\n  Records: {len(rows)} | Delay: {DELAY_SECONDS}s | Port: {PORT}")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((HOST, PORT))
            srv.listen(1)
            print(f"\n  ✓ Server on {HOST}:{PORT} — waiting for client...\n")

            client, addr = srv.accept()
            with client:
                print(f"  ✓ Client connected: {addr}\n")
                sent = 0
                for row in rows:
                    line = format_row_for_stream(row)
                    try:
                        client.sendall(f"{line}\n".encode("utf-8"))
                        sent += 1
                    except BrokenPipeError:
                        print(f"\n  ⚠ Client disconnected after {sent} records.")
                        break
                    if sent % 20 == 0:
                        print(f"    [{sent:>5}/{len(rows)}] region={row.get('region','?')}")
                    time.sleep(DELAY_SECONDS)
                print(f"\n  ✓ Finished streaming {sent} records")

    except KeyboardInterrupt:
        print("\n\n  ✓ Stopped by user (Ctrl+C)")
    except OSError as error:
        print(f"\n  ✗ Socket error: {error}")


if __name__ == "__main__":
    main()
