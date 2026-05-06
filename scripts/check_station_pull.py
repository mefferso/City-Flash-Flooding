"""Quick WeatherSTEM pull test for the station inventory.

Example:
  python scripts/check_station_pull.py --station loyola
  python scripts/check_station_pull.py --all
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from curl_cffi import requests

DEFAULT_STATIONS = Path("data/weatherstem_stations.csv")
DEFAULT_OUTDIR = Path("outputs/check_station_pull")


def pull_station(row: dict, start: str, end: str) -> tuple[bool, str, object]:
    network = row["network"].strip()
    slug = row["slug"].strip()
    url = f"https://{network}.weatherstem.com/data"

    payload = {
        "timezone_offset": 0,
        "id": row["station_id"].strip(),
        "start_date": start,
        "end_date": end,
        "operation": "datapoint",
        "interval": "minute",
        "sensors": [
            row["rain_gauge_sensor_id"].strip(),
            row["rain_rate_sensor_id"].strip(),
        ],
        "format": "json",
        "timestamp_format": "standard",
        "record_id": "1",
        "mysql_mode": False,
        "query": "",
    }

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": f"https://{network}.weatherstem.com",
        "Referer": f"https://{network}.weatherstem.com/data?refer=/{slug}",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            impersonate="chrome",
            timeout=180,
        )
        response.raise_for_status()
        data = response.json()
        return True, response.text, data
    except Exception as exc:  # noqa: BLE001
        return False, str(exc), None


def load_stations(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stations", default=str(DEFAULT_STATIONS))
    parser.add_argument("--station", help="Station slug to test, e.g. loyola")
    parser.add_argument("--all", action="store_true", help="Test all stations in the inventory")
    parser.add_argument("--start", default="2026-05-04 20:25")
    parser.add_argument("--end", default="2026-05-04 21:25")
    parser.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    args = parser.parse_args()

    stations = load_stations(Path(args.stations))

    if args.station:
        stations = [s for s in stations if s.get("slug", "").strip().lower() == args.station.lower()]
    elif not args.all:
        stations = stations[:1]

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    for row in stations:
        print(f"Pulling {row['network']}/{row['slug']} - {row['station_name']}")
        ok, text, data = pull_station(row, args.start, args.end)
        out = outdir / f"{row['network']}_{row['slug']}.json"
        out.write_text(text, encoding="utf-8")

        if not ok:
            print(f"  FAILED: {text}")
            continue

        if isinstance(data, list):
            print(f"  OK: {len(data)} rows saved to {out}")
            if len(data) > 1:
                print(f"  Header: {data[0]}")
                print(f"  First row: {data[1]}")
        else:
            print(f"  Response saved to {out}: {str(data)[:300]}")


if __name__ == "__main__":
    main()
