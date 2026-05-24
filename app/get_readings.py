#!/usr/bin/env python3
"""
get_readings.py - 15-min collection
Supports both EA and SEPA stations
"""
import requests
import psycopg2
from datetime import datetime, timedelta, UTC
import time
from loguru import logger
from river_reference import STATIONS
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

# --------------------------------------------------------------------------- #
# DATABASE
# --------------------------------------------------------------------------- #
def init_db():
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS readings (
            id SERIAL PRIMARY KEY,
            station_id TEXT NOT NULL,
            river TEXT NOT NULL,
            label TEXT NOT NULL,
            level REAL,
            timestamp TEXT NOT NULL,
            UNIQUE(station_id, timestamp)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rainfall_readings (
            id SERIAL PRIMARY KEY,
            level_station_id TEXT NOT NULL,
            rainfall_station_id TEXT NOT NULL,
            rainfall_mm REAL,
            timestamp TEXT NOT NULL,
            UNIQUE(level_station_id, timestamp)
        )
    ''')
    conn.commit()
    conn.close()

# --------------------------------------------------------------------------- #
# API HELPERS
# --------------------------------------------------------------------------- #
def api_get(url, params=None, timeout=10):
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"API error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None

# EA (Environment Agency)
def get_ea_latest_level(station_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    data = api_get(url, {"latest": "", "parameter": "level"})
    if data and 'items' in data and data['items']:
        item = data['items'][0]
        return item.get('value'), item.get('dateTime')
    return None, None

# SEPA (KiWIS API)
def get_sepa_latest_level(station_id):
    url = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"
    params = {
        "service": "kisters",
        "type": "queryServices",
        "datasource": "0",
        "request": "getTimeseriesValues",
        "ts_path": f"1/{station_id}/SG/15m.Cmd",
        "period": "P1D",
        "format": "json"
    }
    data = api_get(url, params)
    
    if not data or not isinstance(data, list) or len(data) == 0:
        logger.warning(f"SEPA returned no data for station {station_id}")
        return None, None

    try:
        for item in data:
            if isinstance(item, dict) and 'data' in item and isinstance(item['data'], list) and len(item['data']) > 0:
                latest_entry = item['data'][-1]
                if len(latest_entry) >= 2:
                    ts = latest_entry[0]
                    value = float(latest_entry[1])
                    logger.info(f"SEPA success for {station_id}: {value:.3f}m at {ts}")
                    return value, ts
    except (IndexError, ValueError, TypeError, KeyError) as e:
        logger.warning(f"SEPA parsing error for {station_id}: {e}")

    logger.warning(f"Could not extract value from SEPA response for {station_id}")
    return None, None

# --------------------------------------------------------------------------- #
# INSERT FUNCTIONS
# --------------------------------------------------------------------------- #
def insert_reading(station_id, river, label, level, timestamp):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO readings (station_id, river, label, level, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (station_id, timestamp) DO NOTHING
        ''', (station_id, river, label, level, timestamp))
        if cursor.rowcount:
            logger.info(f"Inserted level {level:.3f}m for {label} ({station_id})")
        conn.commit()
    except Exception as e:
        logger.error(f"DB insert error: {e}")
    finally:
        conn.close()

def insert_rainfall(level_station_id, rainfall_station_id, rainfall_mm, timestamp):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO rainfall_readings (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (level_station_id, timestamp) DO NOTHING
        ''', (level_station_id, rainfall_station_id, rainfall_mm, timestamp))
        if cursor.rowcount:
            logger.info(f"Inserted rainfall {rainfall_mm}mm for {level_station_id}")
        conn.commit()
    except Exception as e:
        logger.error(f"DB insert error: {e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    init_db()
    logger.info("=== Starting 15-min collection (EA + SEPA) ===")
    logger.info(f"Loaded {len(STATIONS)} rivers from reference")

    # Explicit list of SEPA stations
    SEPA_STATIONS = {"133148", "133170", "133176", "506155"}

    for river, stations in STATIONS.items():
        logger.info(f"Processing river: {river} ({len(stations)} stations)")
        for station in stations:
            sid = station['id']
            label = station['label']
            river_name = river

            logger.info(f" → Fetching {label} ({sid})")

            if sid in SEPA_STATIONS:
                logger.info(f"    Using SEPA API for {sid}")
                level, ts = get_sepa_latest_level(sid)
                source = "SEPA"
            else:
                logger.info(f"    Using EA API for {sid}")
                level, ts = get_ea_latest_level(sid)
                source = "EA"

            if level is not None:
                logger.info(f"    SUCCESS: {label} = {level:.3f}m")
                insert_reading(sid, river_name, label, level, ts)
            else:
                logger.warning(f"    NO DATA for {label} ({sid}) from {source}")

            time.sleep(1)

    logger.info("=== Collection complete ===")