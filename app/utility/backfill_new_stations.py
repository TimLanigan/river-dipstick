#!/usr/bin/env python3
"""
SEPA Backfill for Liddel and Border Esk
"""

from datetime import datetime, timedelta, UTC
import requests
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

def api_get(url, params=None):
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"API error: {e}")
        return None

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
            print(f"Inserted {level:.3f}m for {label} at {timestamp}")
        conn.commit()
    except Exception as e:
        print(f"DB error: {e}")
    finally:
        conn.close()

# SEPA stations to backfill
stations = {
    "133170": ("Liddel", "Newcastleton"),
    "133148": ("Border Esk", "Canonbie")
}

for sid, (river, label) in stations.items():
    print(f"Backfilling 7 days for {label} ({sid})...")
    since = (datetime.now(UTC) - timedelta(days=7)).strftime('%Y-%m-%d')
    
    url = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"
    params = {
        "service": "kisters",
        "type": "queryServices",
        "datasource": "0",
        "request": "getTimeseriesValues",
        "ts_path": f"1/{sid}/SG/15m.Cmd",
        "from": since,
        "format": "json"
    }
    
    data = api_get(url, params)
    count = 0
    if data and isinstance(data, list):
        for entry in data:
            if isinstance(entry, list) and len(entry) >= 2:
                try:
                    ts = entry[0]
                    level = float(entry[1])
                    insert_reading(sid, river, label, level, ts)
                    count += 1
                except:
                    pass
    print(f"  → Inserted {count} readings for {label}")

print("SEPA backfill complete")