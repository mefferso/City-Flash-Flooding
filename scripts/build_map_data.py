"""Build GeoJSON and dashboard-ready CSV files from WeatherSTEM enrichment outputs."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SUMMARY = Path("outputs/flash_flood_weatherstem_summary.csv")
DEFAULT_METRICS = Path("outputs/event_station_metrics.csv")
DEFAULT_STATIONS = Path("data/weatherstem_stations.csv")
DEFAULT_DOCS_DATA = Path("docs/data")

METRIC_COLUMNS = [
    "event_total_in",
    "peak_rain_rate_inhr",
    "max_5min_in",
    "max_15min_in",
    "max_30min_in",
    "max_60min_in",
    "max_180min_in",
    "rain_1hr_before_report_in",
    "rain_2hr_before_report_in",
    "rain_3hr_before_report_in",
    "rain_6hr_before_report_in",
]


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return round(value, 4)
    return value


def to_float(value: Any) -> float | None:
    try:
        if pd.isna(value) or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def dataframe_to_point_geojson(df: pd.DataFrame, lat_col: str, lon_col: str) -> dict[str, Any]:
    features = []
    for _, row in df.iterrows():
        lat = to_float(row.get(lat_col))
        lon = to_float(row.get(lon_col))
        if lat is None or lon is None:
            continue
        props = {str(col): clean_value(row[col]) for col in df.columns if col not in [lat_col, lon_col]}
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def build_threshold_grid(df: pd.DataFrame, grid_size: float = 0.025) -> dict[str, Any]:
    work = df.copy()
    work["event_lat"] = pd.to_numeric(work.get("event_lat"), errors="coerce")
    work["event_lon"] = pd.to_numeric(work.get("event_lon"), errors="coerce")
    work["coverage_pct"] = pd.to_numeric(work.get("coverage_pct"), errors="coerce")
    work["distance_mi"] = pd.to_numeric(work.get("distance_mi"), errors="coerce")

    for col in METRIC_COLUMNS:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    # Conservative defaults for threshold climatology: keep decent gauge coverage and not-too-distant station matches.
    work = work.dropna(subset=["event_lat", "event_lon"])
    work = work[(work["coverage_pct"].fillna(0) >= 80) & (work["distance_mi"].fillna(999) <= 5)]

    if work.empty:
        return {"type": "FeatureCollection", "features": []}

    work["grid_lat"] = (work["event_lat"] / grid_size).round() * grid_size
    work["grid_lon"] = (work["event_lon"] / grid_size).round() * grid_size

    features = []
    for (grid_lat, grid_lon), group in work.groupby(["grid_lat", "grid_lon"]):
        props: dict[str, Any] = {
            "event_count": int(len(group)),
            "grid_lat": round(float(grid_lat), 5),
            "grid_lon": round(float(grid_lon), 5),
        }
        for col in METRIC_COLUMNS:
            if col not in group.columns:
                continue
            vals = group[col].dropna()
            if vals.empty:
                continue
            props[f"median_{col}"] = round(float(vals.median()), 3)
            props[f"p25_{col}"] = round(float(vals.quantile(0.25)), 3)
            props[f"p10_{col}"] = round(float(vals.quantile(0.10)), 3)
            props[f"min_{col}"] = round(float(vals.min()), 3)
            props[f"max_{col}"] = round(float(vals.max()), 3)

        half = grid_size / 2
        west = float(grid_lon - half)
        east = float(grid_lon + half)
        south = float(grid_lat - half)
        north = float(grid_lat + half)
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[west, south], [east, south], [east, north], [west, north], [west, south]]],
                },
                "properties": props,
            }
        )

    return {"type": "FeatureCollection", "features": features}


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build map-ready data files for the city flash flooding dashboard.")
    parser.add_argument("--summary", default=str(DEFAULT_SUMMARY))
    parser.add_argument("--metrics", default=str(DEFAULT_METRICS))
    parser.add_argument("--stations", default=str(DEFAULT_STATIONS))
    parser.add_argument("--outdir", default=str(DEFAULT_DOCS_DATA))
    parser.add_argument("--grid-size", type=float, default=0.025, help="Grid size in decimal degrees.")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    summary = pd.read_csv(args.summary, dtype=str)
    metrics = pd.read_csv(args.metrics, dtype=str)
    stations = pd.read_csv(args.stations, dtype=str)

    # Mirror CSVs into docs/data so GitHub Pages can fetch them directly.
    summary.to_csv(outdir / "flash_flood_weatherstem_summary.csv", index=False)
    metrics.to_csv(outdir / "event_station_metrics.csv", index=False)
    stations.to_csv(outdir / "weatherstem_stations.csv", index=False)

    write_json(outdir / "flash_flood_events.geojson", dataframe_to_point_geojson(summary, "event_lat", "event_lon"))
    write_json(outdir / "event_station_metrics.geojson", dataframe_to_point_geojson(metrics, "event_lat", "event_lon"))
    write_json(outdir / "weatherstem_stations.geojson", dataframe_to_point_geojson(stations, "lat", "lon"))
    write_json(outdir / "rainfall_threshold_grid.geojson", build_threshold_grid(summary, args.grid_size))

    print(f"Wrote dashboard data files to {outdir}")


if __name__ == "__main__":
    main()
