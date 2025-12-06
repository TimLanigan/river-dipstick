#!/usr/bin/env python3
"""
wintermute_fetch.py - FINAL FIXED VERSION (commits everything)
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
import pandas as pd
import argparse

# ------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--apply", action="store_true")
parser.add_argument("--dry-run", dest="apply", action="store_false")
parser.set_defaults(apply=False)
args = parser.parse_args()
DRY_RUN = not args.apply

if DRY_RUN:
    logger.warning("DRY RUN MODE - No changes will be made")
else:
    logger.success("LIVE MODE - Keeping the perfect 15-min grid alive")

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
if not DB_PASS:
    raise ValueError("DB_PASSWORD missing in .env")

CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

# ------------------------------------------------------------------
# Load rules
# ------------------------------------------------------------------
RULES_PATH = Path("/app/data/rules.json")
if not RULES_PATH.exists():
    RULES_PATH = Path("/home/river_levels_app/rules.json")
try:
    RULES = json.loads(RULES_PATH.read_text())
except Exception:
    RULES = {}

# ------------------------------------------------------------------
# DATABASE INIT
# ------------------------------------------------------------------
def init_db():
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()
    cur.execute('''
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
    cur.execute('''
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

# ------------------------------------------------------------------
# API HELPERS
# ------------------------------------------------------------------
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

def get_latest_river_level(station_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    data = api_get(url, params={"latest": "", "parameter": "level"})
    if data and 'items' in data and data['items']:
        item = data['items'][0]
        return item['value'], item['dateTime']
    return None, None

def get_latest_rainfall(rainfall_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{rainfall_id}/readings"
    data = api_get(url, params={"latest": "", "parameter": "rainfall"})
    if data and 'items' in data and data['items']:
        item = data['items'][0]
        return item['value'], item['dateTime']
    return None, None

# ------------------------------------------------------------------
# RAINFALL INSERT
# ------------------------------------------------------------------
def insert_rainfall(level_station_id, rainfall_station_id, rainfall_mm, timestamp):
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()
    try:
        cur.execute('''
            INSERT INTO rainfall_readings
            (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (level_station_id, timestamp) DO NOTHING
        ''', (level_station_id, rainfall_station_id, rainfall_mm, timestamp))
        if cur.rowcount:
            logger.info(f"Inserted {rainfall_mm}mm rain for {level_station_id}")
        conn.commit()
    except Exception as e:
        logger.error(f"Rain insert error: {e}")
    finally:
        cur.close()
        conn.close()

# ------------------------------------------------------------------
# G-SPOT
# ------------------------------------------------------------------
def evaluate_g_spot(cursor, station_id: str, ts_iso: str, current_level: float):
    cfg = RULES.get(station_id, {}).get("good_fishing")
    if not cfg:
        return
    start_lvl = cfg.get("falling_start", 999)
    end_lvl = cfg.get("falling_end", 0)
    rain_thr = cfg.get("rain_threshold", 0)
    ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    two_h_ago = ts - timedelta(hours=2)
    fourteen_d_ago = ts - timedelta(days=14)
    cursor.execute("""
        SELECT level FROM readings
        WHERE station_id = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (station_id, two_h_ago, ts_iso))
    recent = [r[0] for r in cursor.fetchall()]
    falling = len(recent) >= 4 and all(recent[i] >= recent[i+1] for i in range(len(recent)-1))
    in_band = end_lvl <= current_level <= start_lvl
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
        logger.success(f"G SPOT -> {flag.upper()} | {station_id} {current_level:.3f}m")

def evaluate_g_spot_on_real_reading(station_id: str, ts_iso: str, current_level: float):
    if DRY_RUN:
        return
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()
    try:
        evaluate_g_spot(cur, station_id, ts_iso, current_level)
        conn.commit()
    finally:
        cur.close()
        conn.close()

# ------------------------------------------------------------------
# SAFE GRID INSERT - COMMITS THE REAL READING IMMEDIATELY
# ------------------------------------------------------------------
def insert_reading_with_safe_grid(station_id, river, label, level, timestamp_iso):
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()

    # Insert the real reading
    cur.execute("""
        INSERT INTO readings (station_id, river, label, level, timestamp, good_level)
        VALUES (%s, %s, %s, %s, %s, 'n')
        ON CONFLICT (station_id, timestamp) DO NOTHING
    """, (station_id, river, label, level, timestamp_iso))

    if cur.rowcount == 0:
        conn.close()
        return

    logger.info(f"New real reading -> {level:.3f}m @ {label}")

    # COMMIT THE REAL READING RIGHT NOW
    conn.commit()

    # Interpolation (optional)
    ts_dt = datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
    lookback = (ts_dt - timedelta(minutes=45)).isoformat()

    cur.execute("""
        SELECT timestamp, level
        FROM readings
        WHERE station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """, (station_id, lookback))
    rows = cur.fetchall()

    if len(rows) >= 2:
        df = pd.DataFrame(rows, columns=['timestamp', 'level'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()

        start = df.index.min().floor('15min')
        end = df.index.max().ceil('15min')
        perfect = pd.date_range(start=start, end=end, freq='15min')
        df_full = df.reindex(perfect)

        if df_full['level'].isna().sum() > 0:
            df_full['level'] = df_full['level'].interpolate(method='linear').round(3)
            to_insert = []
            for ts, row in df_full.iterrows():
                if ts in df.index or pd.isna(row['level']):
                    continue
                level_float = float(row['level'])
                ts_str = ts.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                to_insert.append((station_id, river, label, level_float, ts_str))

            if to_insert:
                if DRY_RUN:
                    logger.info(f"   Would insert {len(to_insert)} interpolated row(s) for {label}")
                else:
                    for rec in to_insert:
                        cur.execute("""
                            INSERT INTO readings (station_id, river, label, level, timestamp, good_level)
                            VALUES (%s, %s, %s, %s, %s, 'n')
                            ON CONFLICT (station_id, timestamp) DO NOTHING
                        """, rec)
                    logger.success(f"   Inserted {len(to_insert)} interpolated row(s) for {label}")
                    conn.commit()

    conn.close()
    evaluate_g_spot_on_real_reading(station_id, timestamp_iso, level)

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
if __name__ == "__main__":
    init_db()
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    logger.info(f"Starting 15-min collection - {mode}")

    for river, stations in STATIONS.items():
        for station in stations:
            sid = station['id']
            label = station['label']
            rain_id = station.get('rainfall_id')

            level, ts = get_latest_river_level(sid)
            if level is not None and ts is not None:
                insert_reading_with_safe_grid(sid, river, label, level, ts)

            if rain_id:
                rain, rts = get_latest_rainfall(rain_id)
                if rain is not None:
                    insert_rainfall(sid, rain_id, rain, rts or ts)

            time.sleep(1)

    logger.success(f"Collection complete - {mode} - grid perfect!")