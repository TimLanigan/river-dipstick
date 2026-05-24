#!/usr/bin/env python3
"""
river_reference.py
Central station loader for River Dipstick
"""
from pathlib import Path
import csv
import json
import sys
from typing import Dict, List
from loguru import logger

# In Docker, data is mounted to /app/data
CSV_PATH = Path("/app/data/stations.csv")

def load_stations():
    """Load stations from CSV with safe handling"""
    STATIONS = {}
    
    try:
        with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                river = row.get("river", "").strip()
                sid = row.get("station_id", "").strip()
                label = row.get("label", "").strip()
                
                # Safe lat/lon
                try:
                    lat = float(row.get("lat", "").strip()) if row.get("lat", "").strip() else None
                    lon = float(row.get("lon", "").strip()) if row.get("lon", "").strip() else None
                except (ValueError, TypeError):
                    lat = lon = None
                
                # Safe rainfall_id (this was causing the None.strip() error)
                rainfall_raw = row.get("rainfall_id")
                rainfall_id = rainfall_raw.strip() if rainfall_raw and isinstance(rainfall_raw, str) and rainfall_raw.strip() else None
                
                if not river or not sid or not label:
                    continue
                    
                if river not in STATIONS:
                    STATIONS[river] = []
                    
                STATIONS[river].append({
                    "id": sid,
                    "label": label,
                    "lat": lat,
                    "lon": lon,
                    "rainfall_id": rainfall_id
                })
                
    except Exception as e:
        logger.error(f"Error loading stations.csv: {e}")
        return {}

    # Sort rivers alphabetically for consistent tab order
    sorted_stations = dict(sorted(STATIONS.items()))
    return sorted_stations


# --------------------------------------------------------------------------- #
# AUTO-LOAD
# --------------------------------------------------------------------------- #
STATIONS = load_stations()

# --------------------------------------------------------------------------- #
# QUICK TEST
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import pprint
    pprint.pprint(STATIONS)
    sys.exit(0)