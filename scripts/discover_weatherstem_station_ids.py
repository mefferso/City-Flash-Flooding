"""Discover WeatherSTEM station and rain sensor IDs from the TODO inventory.

This script is intentionally conservative. It does not overwrite the active
station inventory directly. It writes candidate rows that should be reviewed
before promoting them into data/weatherstem_stations.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from pathlib import Path
from typing import Any

from curl_cffi import requests

DEFAULT_TODO = Path("data/weatherstem_station_inventory_todo.csv")
DEFAULT_OUT = Path("outputs/weatherstem_station_inventory_candidates.csv")
DEFAULT_RAW_DIR = Path("outputs/weatherstem_discovery_raw")

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/147 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
}

OUT_FIELDS = [
    "parish",
    "network",
    "station_name",
    "slug",
    "station_id",
    "rain_gauge_sensor_id",
    "rain_rate_sensor_id",
    "lat",
    "lon",
    "oldest_record",
    "status",
    "notes",
]


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def to_float(value: Any) -> float | None:
    try:
        if clean(value) == "":
            return None
        return float(value)
    except Exception:
        return None


def flatten(obj: Any, prefix: str = "") -> list[tuple[str, Any]]:
    items: list[tuple[str, Any]] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            items.extend(flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            items.extend(flatten(value, f"{prefix}[{i}]"))
    else:
        items.append((prefix, obj))
    return items


def fetch_text(url: str, timeout: int = 60) -> str | None:
    try:
        resp = requests.get(url, headers=COMMON_HEADERS, impersonate="chrome", timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception:
        return None


def fetch_json(url: str, timeout: int = 60) -> Any | None:
    try:
        resp = requests.get(url, headers=COMMON_HEADERS, impersonate="chrome", timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def find_lat_lon(obj: Any) -> tuple[str, str]:
    lat = ""
    lon = ""
    for key, value in flatten(obj):
        kl = key.lower()
        val = to_float(value)
        if val is None:
            continue
        if not lat and ("lat" in kl or "latitude" in kl) and 27.0 <= val <= 33.5:
            lat = f"{val:.6f}".rstrip("0").rstrip(".")
        if not lon and ("lon" in kl or "lng" in kl or "longitude" in kl) and -94.0 <= val <= -87.0:
            lon = f"{val:.6f}".rstrip("0").rstrip(".")
    return lat, lon


def find_station_id(obj: Any, html_text: str = "") -> str:
    # Prefer explicit station/model IDs from JSON-like metadata.
    for key, value in flatten(obj):
        kl = key.lower()
        if any(token in kl for token in ["station_id", "stationid", "model.id", "id"]):
            s = clean(value)
            if s.isdigit() and 1 <= len(s) <= 8:
                return s

    # Fallback regexes from station/data pages.
    patterns = [
        r'"id"\s*:\s*"?(\d{2,8})"?',
        r"station[_-]?id['\"]?\s*[:=]\s*['\"]?(\d{2,8})",
        r"data-station-id=['\"](\d{2,8})['\"]",
    ]
    for pat in patterns:
        m = re.search(pat, html_text, re.I)
        if m:
            return m.group(1)
    return ""


def sensor_candidates_from_obj(obj: Any) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for key, value in flatten(obj):
        kl = key.lower()
        s = clean(value)
        if not s:
            continue
        if "rain" in kl and s.isdigit():
            candidates.append({"sensor_id": s, "source_key": key, "label": key})
        if s.lower() in ["rain gauge", "rain rate", "rain"]:
            candidates.append({"sensor_id": "", "source_key": key, "label": s})
    return candidates


def sensor_candidates_from_text(text: str) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    # Look for compact JSON snippets where Rain Gauge / Rain Rate are near an ID.
    rain_blocks = re.findall(r".{0,160}Rain\s*(?:Gauge|Rate).{0,160}", text, flags=re.I | re.S)
    for block in rain_blocks:
        ids = re.findall(r'(?:(?:sensor|id|sensor_id)["\']?\s*[:=]\s*["\']?)(\d{3,8})', block, flags=re.I)
        loose_ids = re.findall(r'\b(\d{4,8})\b', block)
        label = "Rain Rate" if re.search(r"Rain\s*Rate", block, re.I) else "Rain Gauge" if re.search(r"Rain\s*Gauge", block, re.I) else "Rain"
        for sid in ids or loose_ids:
            candidates.append({"sensor_id": sid, "source_key": "html_near_rain", "label": label})

    return candidates


def choose_rain_sensors(candidates: list[dict[str, str]]) -> tuple[str, str, str]:
    gauge = ""
    rate = ""
    notes: list[str] = []

    for cand in candidates:
        sid = clean(cand.get("sensor_id"))
        label = clean(cand.get("label") or cand.get("source_key"))
        if not sid or not sid.isdigit():
            continue
        ll = label.lower()
        if not gauge and "rain" in ll and "rate" not in ll:
            gauge = sid
        if not rate and "rain" in ll and "rate" in ll:
            rate = sid

    # WeatherSTEM sensor IDs often appear adjacent as gauge/rate. If we only found
    # unlabeled rain candidates, keep them as review candidates instead of guessing.
    unique_ids = []
    for cand in candidates:
        sid = clean(cand.get("sensor_id"))
        if sid.isdigit() and sid not in unique_ids:
            unique_ids.append(sid)
    if not gauge and len(unique_ids) >= 1:
        notes.append(f"candidate_sensor_ids={';'.join(unique_ids[:8])}")
    if not rate and len(unique_ids) >= 2:
        notes.append("rain gauge/rate labels not confidently identified")

    return gauge, rate, "; ".join(notes)


def discover_station(row: dict[str, str], raw_dir: Path, sleep_s: float = 0.5) -> dict[str, str]:
    network = clean(row.get("network"))
    slug = clean(row.get("slug"))
    station_name = clean(row.get("station_name"))

    out = {field: clean(row.get(field)) for field in OUT_FIELDS}
    out["status"] = "needs_review"
    notes: list[str] = []

    urls = [
        f"https://cdn.weatherstem.com/dashboard/data/dynamic/model/{network}/{slug}/station.json",
        f"https://cdn.weatherstem.com/dashboard/data/dynamic/model/{network}/{slug}/sensors.json",
        f"https://cdn.weatherstem.com/dashboard/data/dynamic/model/{network}/{slug}/model.json",
        f"https://{network}.weatherstem.com/data?refer=/{slug}",
        f"https://{network}.weatherstem.com/{slug}",
    ]

    json_objs: list[Any] = []
    html_texts: list[str] = []

    raw_dir.mkdir(parents=True, exist_ok=True)
    for url in urls:
        time.sleep(sleep_s)
        if url.endswith(".json"):
            obj = fetch_json(url)
            if obj is not None:
                json_objs.append(obj)
                (raw_dir / f"{network}_{slug}_{Path(url).name}").write_text(json.dumps(obj, indent=2), encoding="utf-8")
                notes.append(f"fetched {Path(url).name}")
        else:
            text = fetch_text(url)
            if text:
                html_texts.append(text)
                safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", url.split("//", 1)[-1])[:120]
                (raw_dir / f"{network}_{slug}_{safe}.html").write_text(text, encoding="utf-8")
                notes.append(f"fetched {url}")

    merged = {"json": json_objs}
    if not out.get("lat") or not out.get("lon"):
        for obj in json_objs:
            lat, lon = find_lat_lon(obj)
            if lat and lon:
                out["lat"] = out.get("lat") or lat
                out["lon"] = out.get("lon") or lon
                break

    if not out.get("station_id"):
        for obj in json_objs:
            sid = find_station_id(obj)
            if sid:
                out["station_id"] = sid
                break
        if not out.get("station_id"):
            for text in html_texts:
                sid = find_station_id(merged, text)
                if sid:
                    out["station_id"] = sid
                    break

    sensor_cands: list[dict[str, str]] = []
    for obj in json_objs:
        sensor_cands.extend(sensor_candidates_from_obj(obj))
    for text in html_texts:
        sensor_cands.extend(sensor_candidates_from_text(text))

    if not out.get("rain_gauge_sensor_id") or not out.get("rain_rate_sensor_id"):
        gauge, rate, sensor_notes = choose_rain_sensors(sensor_cands)
        if not out.get("rain_gauge_sensor_id") and gauge:
            out["rain_gauge_sensor_id"] = gauge
        if not out.get("rain_rate_sensor_id") and rate:
            out["rain_rate_sensor_id"] = rate
        if sensor_notes:
            notes.append(sensor_notes)

    required = ["station_id", "rain_gauge_sensor_id", "rain_rate_sensor_id", "lat", "lon"]
    if all(out.get(k) for k in required):
        out["status"] = "candidate_complete"
    elif out.get("station_id") or out.get("lat") or out.get("rain_gauge_sensor_id") or out.get("rain_rate_sensor_id"):
        out["status"] = "partial"
    else:
        out["status"] = "not_found"

    out["notes"] = "; ".join(dict.fromkeys([n for n in notes if n]))
    print(f"{network}/{slug} - {station_name}: {out['status']}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover WeatherSTEM station/sensor IDs for TODO inventory rows.")
    parser.add_argument("--todo", default=str(DEFAULT_TODO))
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sleep", type=float, default=0.5)
    args = parser.parse_args()

    todo_path = Path(args.todo)
    out_path = Path(args.out)
    raw_dir = Path(args.raw_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with todo_path.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    if args.limit:
        rows = rows[: args.limit]

    discovered = [discover_station(row, raw_dir, sleep_s=args.sleep) for row in rows]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        writer.writeheader()
        writer.writerows(discovered)

    complete = sum(1 for r in discovered if r["status"] == "candidate_complete")
    partial = sum(1 for r in discovered if r["status"] == "partial")
    print(f"Wrote {len(discovered)} rows to {out_path}")
    print(f"Complete candidates: {complete}; partial candidates: {partial}")


if __name__ == "__main__":
    main()
