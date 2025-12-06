#!/usr/bin/env python3
"""
get_readings.py - 15-min collection
Now with permanent, error-free G SPOT detection
"""

import requests
import psycopg2
from datetime import datetime, timedelta, UTC
import time
import json
from pathlib import Path
from loguru import logger
from river_reference import STATIONS
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

# === LOAD RULES ===
RULES_PATH = Path("/app/data/rules.json")
if not RULES_PATH.exists():
    RULES_PATH = Path("/home/river_levels_app/rules.json")
try:
    RULES = json.loads(RULES_PATH.read_text())
except Exception as e:
    logger.error(f"Failed to load rules.json: {e}")
    RULES = {}

# === DATABASE ===
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
            good_level TEXT DEFAULT 'n',
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

# === API ===
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

# === LEVELS ===
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
            INSERT INTO readings (station_id, river, label, level, timestamp, good_level)
            VALUES (%s, %s, %s, %s, %s, 'n')
            ON CONFLICT (station_id, timestamp) DO NOTHING
        ''', (station_id, river, label, level, timestamp))

        if cursor.rowcount:
            logger.info(f"Inserted {level:.3f}m @ {label}")
            try:
                evaluate_g_spot(cursor, station_id, timestamp, level)
            except Exception as e:
                logger.error(f"G SPOT eval failed: {e}")

        conn.commit()
    except Exception as e:
        logger.error(f"Insert failed: {e}")
    finally:
        cursor.close()
        conn.close()

# === G SPOT EVALUATION ===
def evaluate_g_spot(cursor, station_id: str, ts_iso: str, current_level: float):
    cfg = RULES.get(station_id, {}).get("good_fishing")
    if not cfg:
        return

    start_lvl = cfg.get("falling_start", 999)
    end_lvl   = cfg.get("falling_end",   0)
    rain_thr  = cfg.get("rain_threshold", 0)

    ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    two_h_ago      = ts - timedelta(hours=2)
    fourteen_d_ago = ts - timedelta(days=14)

    # Falling?
    cursor.execute("""
        SELECT level FROM readings
        WHERE station_id = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (station_id, two_h_ago, ts_iso))
    recent = [r[0] for r in cursor.fetchall()]
    falling = len(recent) >= 4 and all(recent[i] >= recent[i+1] for i in range(len(recent)-1))

    # In band?
    in_band = end_lvl <= current_level <= start_lvl

    # 14-day rain (TEXT column)
    cursor.execute("""
        SELECT COALESCE(SUM(rainfall_mm), 0)
        FROM rainfall_readings
        WHERE level_station_id = %s AND timestamp::timestamptz >= %s
    """, (station_id, fourteen_d_ago))
    rain_total = cursor.fetchone()[0] or 0
    rain_ok = rain_total >= rain_thr

    flag = 'y' if (falling and in_band and rain_ok) else 'n'

    cursor.execute("""
        UPDATE readings SET good_level = %s
        WHERE station_id = %s AND timestamp = %s
    """, (flag, station_id, ts_iso))

    if cursor.rowcount:
        logger.success(f"G SPOT → {flag.upper()} | {station_id} {current_level:.3f}m")

# === RAINFALL ===
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
            INSERT INTO rainfall_readings
            (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (level_station_id, timestamp) DO NOTHING
        ''', (level_station_id, rainfall_station_id, rainfall_mm, timestamp))
        if cursor.rowcount:
            logger.info(f"Inserted {rainfall_mm}mm rain for {level_station_id}")
        conn.commit()
    except Exception as e:
        logger.error(f"Rain insert error: {e}")
    finally:
        cursor.close()
        conn.close()

# === GAPS ===
def has_gaps(station_id, days=7):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    since = (datetime.now(UTC) - timedelta(hours=24)).replace(microsecond=0).isoformat()
    cursor.execute("SELECT COUNT(*) FROM readings WHERE station_id = %s AND timestamp >= %s", (station_id, since))
    count = cursor.fetchone()[0]
    expected = days * 96
    conn.close()
    return count < expected * 0.9

# === MAIN ===
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

            # Gap backfill
            if has_gaps(sid):
                logger.warning(f"Gaps in {sid} — backfilling 2 days")
                since = (datetime.now(UTC) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
                for l, t in fetch_missing_readings(sid, since):
                    insert_reading(sid, river, label, l, t)
                if rain_id:
                    for r, t in fetch_missing_rainfall(rain_id, since):
                        insert_rainfall(sid, rain_id, r, t)

            time.sleep(1)

    logger.info("Collection complete — G SPOTs updated")