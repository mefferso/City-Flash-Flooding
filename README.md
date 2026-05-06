[README.md](https://github.com/user-attachments/files/27444936/README.md)
# Urban Flash Flooding WeatherSTEM Linker

This repo links flash flood reports in the LIX area to high-resolution WeatherSTEM rainfall observations.

The initial focus parishes are:

- Orleans
- Jefferson
- St. Charles
- East Baton Rouge

The goal is to build a repeatable enrichment workflow that answers:

> What observed rainfall rates/totals occurred near each flash flood report?

## What the script calculates

For every flash flood report and every nearby usable WeatherSTEM station, the enrichment script pulls WeatherSTEM minute rainfall data for:

```text
T - 6 hours through T + 1 hour
```

Then it calculates:

- event-window rainfall total
- peak rain rate
- max 5-minute rainfall
- max 15-minute rainfall
- max 30-minute rainfall
- max 60-minute rainfall
- max 180-minute rainfall
- station distance from report
- station/report confidence flag
- basic QC flags for rainfall resets and suspicious 1-minute jumps

## Important WeatherSTEM implementation notes

WeatherSTEM's data endpoint uses a weird-but-working browser-style request:

```text
POST https://{network}.weatherstem.com/data
```

The body is JSON, even though the browser advertises it as form encoded:

```json
{
  "timezone_offset": 0,
  "id": "1819",
  "start_date": "2026-05-04 20:25",
  "end_date": "2026-05-05 20:25",
  "operation": "datapoint",
  "interval": "minute",
  "sensors": ["37688", "37691"],
  "format": "json",
  "timestamp_format": "standard",
  "record_id": "1",
  "mysql_mode": false,
  "query": ""
}
```

Python `requests` had SSL/TLS trouble with this site, so the scripts use `curl_cffi` with Chrome impersonation.

## Repo layout

```text
.github/workflows/run_weatherstem_enrichment.yml
scripts/check_station_pull.py
scripts/enrich_weatherstem_flash_floods.py
data/flash_flood_events_focus_parishes.csv
data/weatherstem_stations.csv
data/weatherstem_station_inventory_todo.csv
outputs/
weatherstem_cache/
```

## Quick start locally

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Test one station:

```bash
python scripts/check_station_pull.py --station loyola
```

Run the enrichment:

```bash
python scripts/enrich_weatherstem_flash_floods.py \
  --events data/flash_flood_events_focus_parishes.csv \
  --stations data/weatherstem_stations.csv \
  --radius 5 \
  --min-date 2022-01-01
```

Outputs are written to:

```text
outputs/event_station_metrics.csv
outputs/flash_flood_weatherstem_summary.csv
```

## Current station inventory status

Confirmed starter stations are in `data/weatherstem_stations.csv`:

| Parish | Station | Network | Slug | Station ID | Rain Gauge | Rain Rate |
|---|---|---|---|---:|---:|---:|
| Orleans | Loyola University New Orleans - Uptown/Audubon | orleans | loyola | 1819 | 37688 | 37691 |
| East Baton Rouge | LSU Alex Box Stadium | eastbatonrouge | alexbox | 1313 | 27974 | 27977 |
| St. Charles | St. Charles Parish EOC | stcharles | stcharleseoc | 1884 | 38955 | 38958 |

The full TODO inventory is in:

```text
data/weatherstem_station_inventory_todo.csv
```

The next job is filling in the remaining station IDs and rain sensor IDs from the WeatherSTEM Data Mining payloads.

## GitHub Actions

The workflow can be run manually from the **Actions** tab. It installs dependencies, runs the enrichment script, and uploads the output CSVs as an artifact.

## Caveats

- Many flash flood reports predate WeatherSTEM station installation. The workflow defaults to `--min-date 2022-01-01` to avoid wasting calls on old events.
- The starter station coordinates are estimates and should be refined as station metadata is confirmed.
- Rain Gauge appears to behave like a cumulative gauge value, so the script derives interval rainfall from gauge differences rather than summing gauge values directly.
- WeatherSTEM network domains may occasionally be flaky from command-line clients; scripts retry and keep going.
