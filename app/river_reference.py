#!/usr/bin/env python3
"""
riverdipstick/stations.py
Central reference loader – CSV + EA API coordinates.
"""
from pathlib import Path
import csv
import json
import pathlib
import sys
from typing import Dict, List, Tuple

import requests
from loguru import logger

# --------------------------------------------------------------------------- #
# CONFIG – robust path handling
# --------------------------------------------------------------------------- #
# Project root = directory that contains the `riverdipstick` package
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
from pathlib import Path

# In Docker, data is mounted to /app/data
CSV_PATH = Path("/app/data/stations.csv")

def load_stations():
    with open(CSV_PATH, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        stations = {}
        for row in reader:
            station_id = row['Station ID']
            stations[station_id] = {
                'river': row['River'],
                'label': row['Label'],
                'lat': float(row['Latitude']),
                'lon': float(row['Longitude']),
            }
        return stations
CACHE_PATH = DATA_DIR / "station_coords_cache.json"

# Ensure data folder exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# DEFAULT CSV (created on first run if missing)
# --------------------------------------------------------------------------- #
DEFAULT_CSV_CONTENT = """river,station_id,label,lat,lon
Eden,760101,Kirkby Stephen,,
Eden,760112,Great Musgrave Bridge,,
Eden,760115,Appleby,,
Eden,760502,Temple Sowerby,,
Eden,762505,Great Corby,,
Eden,765512,Sheepmount,,
Eden,762540,Linstock,,
Ribble,710151,Locks Weir,,
Ribble,710102,Penny Bridge,,
Ribble,710301,Low Moor,,
Ribble,710305,Henthorn,,
Ribble,713056,New Jumbles Rock,,
Ribble,713040,Ribchester School,,
River Lune,724735,Lancaster Quay,54.051851,-2.822021
River Lune,724629,Caton,54.081344,-2.722026
River Lune,722242,Lunes Bridge,54.420034,-2.599286
River Lune,722421,Killington,54.310113,-2.582471
River Lune,724647,Skerton Weir,54.063594,-2.791879
"""

# --------------------------------------------------------------------------- #
# INTERNAL CACHE
# --------------------------------------------------------------------------- #
def _load_cache() -> Dict[str, Tuple[float, float]]:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}

def _save_cache(cache: Dict[str, Tuple[float, float]]):
    CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding="utf-8")

# --------------------------------------------------------------------------- #
# CORE LOADER
# --------------------------------------------------------------------------- #
def load_stations():
    import csv
    import json
    import os
    from loguru import logger

    CSV_PATH = "/app/data/stations.csv"
    CACHE_PATH = "/app/data/station_coords_cache.json"

    STATIONS = {}

    # Load cache
    cache = {}
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, 'r') as f:
            cache = json.load(f)

    with open(CSV_PATH, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            river = row["river"].strip()
            sid = row["station_id"].strip()
            label = row["label"].strip()
            lat = row["lat"].strip()
            lon = row["lon"].strip()
            rainfall_id = row.get("rainfall_id", "").strip() or None  # ← CRITICAL LINE

            # Use CSV lat/lon if available
            if lat and lon:
                lat, lon = float(lat), float(lon)
                cache[sid] = (lat, lon)
                logger.debug(f"CSV hit for {sid} → {lat},{lon}")
            else:
                # Fallback to cache or API
                if sid in cache:
                    lat, lon = cache[sid]
                    logger.debug(f"Cache hit for {sid} → {lat},{lon}")
                else:
                    lat, lon = fetch_coordinates_from_ea(sid)
                    if lat and lon:
                        cache[sid] = (lat, lon)
                        logger.debug(f"API hit for {sid} → {lat},{lon}")
                    else:
                        lat, lon = None, None
                        logger.warning(f"No coordinates for {sid}")

            if river not in STATIONS:
                STATIONS[river] = []
            STATIONS[river].append({
                "id": sid,
                "label": label,
                "lat": lat,
                "lon": lon,
                "rainfall_id": rainfall_id  # ← NOW INCLUDED
            })

    # Save updated cache
    with open(CACHE_PATH, 'w') as f:
        json.dump(cache, f, indent=2)

    return STATIONS


def _fetch_coords_from_ea(station_id: str) -> Tuple[float | None, float | None]:
    """Pull lat/lon from EA flood-monitoring API."""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}.json"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", {})
        lat = items.get("lat")
        lon = items.get("long")
        if lat is not None and lon is not None:
            logger.info("Fetched {} → {},{}", station_id, lat, lon)
            return float(lat), float(lon)
        logger.warning("EA returned no lat/long for {}", station_id)
        return None, None
    except Exception as exc:
        logger.error("EA API error for {}: {}", station_id, exc)
        return None, None


# --------------------------------------------------------------------------- #
# QUICK TEST
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import pprint
    pprint.pprint(load_stations())
    sys.exit(0)
# === AUTO-LOAD STATIONS FOR IMPORT ===
STATIONS = load_stations()