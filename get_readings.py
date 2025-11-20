#!/usr/bin/env python3
"""
get_readings.py - 15-min collection
Optimized: Fresh data check + 2-day gap backfill + retry + logging
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
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@localhost/river_levels_db'

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
# API WITH RETRY
# --------------------------------------------------------------------------- #
def api_get(url, params=None):
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"API error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None

# --------------------------------------------------------------------------- #
# LEVELS
# --------------------------------------------------------------------------- #
def get_latest_river_level(station_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    data = api_get(url, params={"latest": "", "parameter": "level"})
    if data and 'items' in data and data['items']:
        item = data['items'][0]
        return item['value'], item['dateTime']
    return None, None

def fetch_missing_readings(station_id, since_date):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    data = api_get(url, params={"parameter": "level", "since": since_date, "_sorted": ""})
    if data and 'items' in data:
        return [(item['value'], item['dateTime']) for item in data['items']]
    return []

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

# --------------------------------------------------------------------------- #
# RAINFALL
# --------------------------------------------------------------------------- #
def get_latest_rainfall(rainfall_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{rainfall_id}/readings"
    data = api_get(url, params={"latest": "", "parameter": "rainfall"})
    if data and 'items' in data and data['items']:
        item = data['items'][0]
        return item['value'], item['dateTime']
    return None, None

def fetch_missing_rainfall(rainfall_id, since_date):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{rainfall_id}/readings"
    data = api_get(url, params={"parameter": "rainfall", "since": since_date, "_sorted": ""})
    if data and 'items' in data:
        return [(item['value'], item['dateTime']) for item in data['items']]
    return []

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
            logger.info(f"Inserted rainfall {rainfall_mm}mm for {level_station_id} from {rainfall_station_id}")
        conn.commit()
    except psycopg2.errors.UndefinedObject:
        # Fallback if constraint missing
        cursor.execute('''
            INSERT INTO rainfall_readings (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        ''', (level_station_id, rainfall_station_id, rainfall_mm, timestamp))
        if cursor.rowcount:
            logger.info(f"Inserted rainfall {rainfall_mm}mm for {level_station_id} (fallback)")
        conn.commit()
    except Exception as e:
        logger.error(f"DB insert error: {e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------- #
# GAPS (2 DAYS)
# --------------------------------------------------------------------------- #
def has_gaps(station_id, days=2):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    since = (datetime.now(UTC) - timedelta(days=days)).isoformat() + 'Z'
    cursor.execute("SELECT COUNT(*) FROM readings WHERE station_id = %s AND timestamp >= %s", (station_id, since))
    count = cursor.fetchone()[0]
    expected = days * 96  # 15-min
    conn.close()
    return count < expected * 0.9

# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    init_db()
    logger.info("Starting 15-min collection")

    for river, stations in STATIONS.items():
        for station in stations:
            sid = station['id']
            label = station['label']
            rain_id = station.get('rainfall_id')

            # Level
            level, ts = get_latest_river_level(sid)
            if level is not None:
                insert_reading(sid, river, label, level, ts)

            # Rainfall
            if rain_id:
                rain, rts = get_latest_rainfall(rain_id)
                if rain is not None:
                    insert_rainfall(sid, rain_id, rain, rts or ts)

            # Gap backfill (2 days)
            if has_gaps(sid):
                logger.warning(f"Gaps in {sid} — backfilling 2 days")
                since = (datetime.now(UTC) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
                for l, t in fetch_missing_readings(sid, since):
                    insert_reading(sid, river, label, l, t)
                if rain_id:
                    for r, t in fetch_missing_rainfall(rain_id, since):
                        insert_rainfall(sid, rain_id, r, t)

            time.sleep(1)

    logger.info("Collection complete")