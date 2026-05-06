"""Link WeatherSTEM minute rainfall data to flash flood reports.

This script reads a WeatherSTEM station inventory and a flash flood report CSV,
then calculates rainfall metrics around each flash flood report.

Default event window: T-6 hours to T+1 hour.
Default search radius: 5 miles.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from curl_cffi import requests

DURATIONS_MIN = [5, 15, 30, 60, 180]
WINDOW_HOURS_BEFORE = 6
WINDOW_HOURS_AFTER = 1

DEFAULT_EVENTS = Path("data/flash_flood_events_focus_parishes.csv")
DEFAULT_STATIONS = Path("data/weatherstem_stations.csv")
DEFAULT_OUTPUT_DIR = Path("outputs")
DEFAULT_CACHE_DIR = Path("weatherstem_cache")


def clean_str(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def to_float(value: Any) -> float | None:
    try:
        if value is None or clean_str(value) == "":
            return None
        return float(value)
    except Exception:
        return None


def safe_name(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("_")


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_mi = 3958.7613
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius_mi * math.asin(math.sqrt(a))


def flatten_json(obj: Any, prefix: str = "") -> list[tuple[str, Any]]:
    items: list[tuple[str, Any]] = []
    if isinstance(obj, dict):
        for key, val in obj.items():
            joined = f"{prefix}.{key}" if prefix else str(key)
            items.extend(flatten_json(val, joined))
    elif isinstance(obj, list):
        for idx, val in enumerate(obj):
            items.extend(flatten_json(val, f"{prefix}[{idx}]"))
    else:
        items.append((prefix, obj))
    return items


def find_lat_lon_in_json(obj: Any) -> tuple[float | None, float | None]:
    flat = flatten_json(obj)
    lat_candidates: list[tuple[str, float]] = []
    lon_candidates: list[tuple[str, float]] = []

    for key, value in flat:
        key_l = key.lower()
        val = to_float(value)
        if val is None:
            continue
        if ("lat" in key_l or "latitude" in key_l) and 27.0 <= val <= 33.5:
            lat_candidates.append((key, val))
        if ("lon" in key_l or "lng" in key_l or "longitude" in key_l) and -94.0 <= val <= -87.0:
            lon_candidates.append((key, val))

    if lat_candidates and lon_candidates:
        return lat_candidates[0][1], lon_candidates[0][1]
    return None, None


def fetch_station_metadata_latlon(network: str, slug: str) -> tuple[float | None, float | None]:
    url = f"https://cdn.weatherstem.com/dashboard/data/dynamic/model/{network}/{slug}/station.json"
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            impersonate="chrome",
            timeout=60,
        )
        if response.status_code != 200:
            return None, None
        return find_lat_lon_in_json(response.json())
    except Exception:
        return None, None


def parse_oldest_record(value: str) -> datetime | None:
    value = clean_str(value)
    if not value:
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def load_stations(stations_csv: Path) -> list[dict[str, Any]]:
    stations: list[dict[str, Any]] = []
    with stations_csv.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            network = clean_str(row.get("network"))
            slug = clean_str(row.get("slug"))
            station_id = clean_str(row.get("station_id"))
            rain_gauge_sensor_id = clean_str(row.get("rain_gauge_sensor_id"))
            rain_rate_sensor_id = clean_str(row.get("rain_rate_sensor_id"))
            if not all([network, slug, station_id, rain_gauge_sensor_id, rain_rate_sensor_id]):
                continue

            lat = to_float(row.get("lat"))
            lon = to_float(row.get("lon"))
            if lat is None or lon is None:
                print(f"Trying station metadata lat/lon for {network}/{slug}...")
                lat, lon = fetch_station_metadata_latlon(network, slug)

            if lat is None or lon is None:
                print(f"WARNING: skipping {network}/{slug}; missing lat/lon")
                continue

            stations.append(
                {
                    "parish": clean_str(row.get("parish")),
                    "network": network,
                    "station_name": clean_str(row.get("station_name")),
                    "slug": slug,
                    "station_id": station_id,
                    "rain_gauge_sensor_id": rain_gauge_sensor_id,
                    "rain_rate_sensor_id": rain_rate_sensor_id,
                    "lat": lat,
                    "lon": lon,
                    "oldest_record": parse_oldest_record(clean_str(row.get("oldest_record"))),
                }
            )
    print(f"Loaded usable WeatherSTEM stations: {len(stations)}")
    return stations


def row_get(row: pd.Series, candidates: list[str]) -> str:
    lookup = {str(key).upper(): row[key] for key in row.index}
    for candidate in candidates:
        value = clean_str(lookup.get(candidate.upper()))
        if value:
            return value
    return ""


def normalize_time_token(value: Any) -> str:
    s = clean_str(value)
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        s = s.zfill(4)
        return f"{s[:2]}:{s[2:]}"
    return s


def parse_event_datetime(row: pd.Series) -> datetime | None:
    for col in ["event_datetime", "datetime", "timestamp", "begin_datetime", "BEGIN_DATE_TIME", "BEGIN_DATETIME"]:
        value = row_get(row, [col])
        if value:
            dt = pd.to_datetime(value, errors="coerce")
            if not pd.isna(dt):
                return dt.to_pydatetime()

    date_value = row_get(row, ["BEGIN_DATE", "begin_date", "EVENT_DATE", "event_date", "date"])
    time_value = row_get(row, ["BEGIN_TIME", "begin_time", "EVENT_TIME", "event_time", "time"])
    if date_value and time_value:
        dt = pd.to_datetime(f"{date_value} {normalize_time_token(time_value)}", errors="coerce")
        if not pd.isna(dt):
            return dt.to_pydatetime()
    return None


def parse_event_latlon(row: pd.Series) -> tuple[float | None, float | None]:
    lat = to_float(row_get(row, ["Latitude", "lat", "latitude", "BEGIN_LAT", "begin_lat", "EVENT_LAT", "event_lat"]))
    lon = to_float(row_get(row, ["Longitude", "lon", "longitude", "lng", "BEGIN_LON", "begin_lon", "BEGIN_LONG", "event_lon", "EVENT_LON"]))
    return lat, lon


def confidence_from_distance(distance_mi: float | None) -> str:
    if distance_mi is None:
        return "poor"
    if distance_mi <= 1:
        return "high"
    if distance_mi <= 3:
        return "medium"
    if distance_mi <= 5:
        return "low"
    return "poor"


def format_weatherstem_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")


def pull_weatherstem_station(station: dict[str, Any], start_dt: datetime, end_dt: datetime, cache_dir: Path) -> Any:
    network = station["network"]
    slug = station["slug"]
    cache_file = cache_dir / f"{network}_{slug}_{start_dt:%Y%m%d%H%M}_{end_dt:%Y%m%d%H%M}.json"

    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            pass

    url = f"https://{network}.weatherstem.com/data"
    payload = {
        "timezone_offset": 0,
        "id": station["station_id"],
        "start_date": format_weatherstem_time(start_dt),
        "end_date": format_weatherstem_time(end_dt),
        "operation": "datapoint",
        "interval": "minute",
        "sensors": [station["rain_gauge_sensor_id"], station["rain_rate_sensor_id"]],
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

    last_error = None
    for attempt in range(1, 4):
        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                impersonate="chrome",
                timeout=180,
            )
            text = response.text
            cache_file.write_text(text, encoding="utf-8")
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"    WeatherSTEM pull failed for {network}/{slug}, attempt {attempt}/3: {exc}")
            time.sleep(5)
    return {"error": str(last_error)}


def compute_rain_metrics(raw_data: Any, window_start: datetime, window_end: datetime) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "records": 0,
        "coverage_pct": None,
        "event_total_in": None,
        "peak_rain_rate_inhr": None,
        "qc_negative_resets": 0,
        "qc_big_1min_increments": 0,
    }
    for duration in DURATIONS_MIN:
        metrics[f"max_{duration}min_in"] = None

    if not isinstance(raw_data, list) or len(raw_data) < 2:
        return metrics

    header = raw_data[0]
    rows = raw_data[1:]
    try:
        df = pd.DataFrame(rows, columns=header)
    except Exception:
        return metrics

    if "Timestamp" not in df.columns or "Rain Gauge" not in df.columns:
        return metrics

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df["Rain Gauge"] = pd.to_numeric(df["Rain Gauge"], errors="coerce")
    df["Rain Rate"] = pd.to_numeric(df["Rain Rate"], errors="coerce") if "Rain Rate" in df.columns else None
    df = df.dropna(subset=["Timestamp", "Rain Gauge"]).drop_duplicates(subset=["Timestamp"]).sort_values("Timestamp")
    if df.empty:
        return metrics

    gauge = df["Rain Gauge"].astype(float)
    increments = gauge.diff()
    increments.iloc[0] = 0.0

    reset_count = int((increments < -0.001).sum())
    increments = increments.mask(increments < -0.001, gauge)
    increments = increments.clip(lower=0)

    big_1min_count = int((increments > 2.0).sum())
    series = pd.Series(increments.values, index=df["Timestamp"]).sort_index()

    expected_minutes = max(1, int((window_end - window_start).total_seconds() / 60) + 1)
    metrics["records"] = int(len(df))
    metrics["coverage_pct"] = round(100.0 * len(df) / expected_minutes, 1)
    metrics["event_total_in"] = round(float(series.sum()), 3)
    if df["Rain Rate"].notna().any():
        metrics["peak_rain_rate_inhr"] = round(float(df["Rain Rate"].max()), 3)
    metrics["qc_negative_resets"] = reset_count
    metrics["qc_big_1min_increments"] = big_1min_count

    for duration in DURATIONS_MIN:
        rolling = series.rolling(f"{duration}min").sum()
        value = rolling.max()
        if pd.notna(value):
            metrics[f"max_{duration}min_in"] = round(float(value), 3)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Link WeatherSTEM rainfall data to flash flood reports.")
    parser.add_argument("--events", default=str(DEFAULT_EVENTS))
    parser.add_argument("--stations", default=str(DEFAULT_STATIONS))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--radius", type=float, default=5.0)
    parser.add_argument("--min-date", default="2022-01-01", help="Skip events before this date. Use blank string to disable.")
    parser.add_argument("--max-events", type=int, default=None, help="Optional limit for quick tests.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    min_dt = None
    if clean_str(args.min_date):
        min_dt = pd.to_datetime(args.min_date, errors="coerce")
        min_dt = None if pd.isna(min_dt) else min_dt.to_pydatetime()

    stations = load_stations(Path(args.stations))
    if not stations:
        raise SystemExit("No usable stations loaded. Add station lat/lon and sensor IDs.")

    events = pd.read_csv(args.events, dtype=str, encoding="utf-8-sig")
    station_metric_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    processed = 0

    print(f"Loaded flash flood report rows: {len(events)}")

    for idx, row in events.iterrows():
        event_id = row_get(row, ["EVENT_ID", "event_id", "id"]) or f"row_{idx + 1}"
        event_dt = parse_event_datetime(row)
        event_lat, event_lon = parse_event_latlon(row)
        parish = row_get(row, ["Parish/County", "parish", "PARISH", "CZ_NAME", "county", "COUNTY"])

        if event_dt is None or event_lat is None or event_lon is None:
            print(f"Skipping event {event_id}: missing datetime or lat/lon")
            continue
        if min_dt and event_dt < min_dt:
            continue

        processed += 1
        if args.max_events and processed > args.max_events:
            break

        window_start = event_dt - timedelta(hours=WINDOW_HOURS_BEFORE)
        window_end = event_dt + timedelta(hours=WINDOW_HOURS_AFTER)
        print(f"\nEvent {event_id}: {event_dt} at {event_lat:.4f}, {event_lon:.4f}")

        nearby: list[tuple[float, dict[str, Any]]] = []
        for station in stations:
            if station.get("oldest_record") and window_end < station["oldest_record"]:
                continue
            dist = haversine_miles(event_lat, event_lon, station["lat"], station["lon"])
            if dist <= args.radius:
                nearby.append((dist, station))
        nearby.sort(key=lambda item: item[0])

        if not nearby:
            summary_rows.append(
                {
                    "event_id": event_id,
                    "event_datetime": event_dt,
                    "event_parish": parish,
                    "event_lat": event_lat,
                    "event_lon": event_lon,
                    "stations_checked": 0,
                    "usable_stations": 0,
                    "nearest_station_confidence": "poor",
                }
            )
            continue

        event_station_results: list[dict[str, Any]] = []
        for dist, station in nearby:
            print(f"  Pulling {station['station_name']} ({dist:.2f} mi)")
            raw = pull_weatherstem_station(station, window_start, window_end, cache_dir)
            if isinstance(raw, dict) and "error" in raw:
                print(f"    ERROR: {raw['error']}")
                continue
            metrics = compute_rain_metrics(raw, window_start, window_end)
            result = {
                "event_id": event_id,
                "event_datetime": event_dt,
                "event_parish": parish,
                "event_lat": event_lat,
                "event_lon": event_lon,
                "station_parish": station["parish"],
                "station_network": station["network"],
                "station_name": station["station_name"],
                "station_slug": station["slug"],
                "station_lat": station["lat"],
                "station_lon": station["lon"],
                "distance_mi": round(dist, 3),
                **metrics,
            }
            station_metric_rows.append(result)
            event_station_results.append(result)
            time.sleep(1)

        usable = [r for r in event_station_results if r.get("records", 0) > 0]
        usable_sorted = sorted(usable, key=lambda r: r["distance_mi"])

        summary: dict[str, Any] = {
            "event_id": event_id,
            "event_datetime": event_dt,
            "event_parish": parish,
            "event_lat": event_lat,
            "event_lon": event_lon,
            "stations_checked": len(event_station_results),
            "usable_stations": len(usable),
        }

        if usable_sorted:
            nearest = usable_sorted[0]
            summary.update(
                {
                    "nearest_station_name": nearest["station_name"],
                    "nearest_station_slug": nearest["station_slug"],
                    "nearest_station_distance_mi": nearest["distance_mi"],
                    "nearest_station_confidence": confidence_from_distance(nearest["distance_mi"]),
                    "nearest_records": nearest["records"],
                    "nearest_coverage_pct": nearest["coverage_pct"],
                    "nearest_event_total_in": nearest["event_total_in"],
                    "nearest_peak_rain_rate_inhr": nearest["peak_rain_rate_inhr"],
                }
            )
            for duration in DURATIONS_MIN:
                summary[f"nearest_max_{duration}min_in"] = nearest.get(f"max_{duration}min_in")

            for radius in [1, 2, 5]:
                within = [r for r in usable if r["distance_mi"] <= radius]
                summary[f"stations_within_{radius}mi"] = len(within)
                for duration in DURATIONS_MIN:
                    values = [r.get(f"max_{duration}min_in") for r in within if r.get(f"max_{duration}min_in") is not None]
                    summary[f"max_{duration}min_within_{radius}mi_in"] = round(max(values), 3) if values else None
                totals = [r.get("event_total_in") for r in within if r.get("event_total_in") is not None]
                summary[f"max_total_within_{radius}mi_in"] = round(max(totals), 3) if totals else None
        else:
            summary.update({"nearest_station_confidence": "poor"})

        summary_rows.append(summary)

    station_out = output_dir / "event_station_metrics.csv"
    summary_out = output_dir / "flash_flood_weatherstem_summary.csv"
    pd.DataFrame(station_metric_rows).to_csv(station_out, index=False)
    pd.DataFrame(summary_rows).to_csv(summary_out, index=False)

    print("\nDone.")
    print(f"Processed events after filters: {processed}")
    print(f"Station-level output: {station_out}")
    print(f"Event summary output:  {summary_out}")


if __name__ == "__main__":
    main()
