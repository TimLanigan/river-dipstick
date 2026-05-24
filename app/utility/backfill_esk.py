#!/usr/bin/env python3
"""
Backfill for Esk (Canonbie)
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

# Backfill Esk / Canonbie
print("Backfilling 7 days for Canonbie (Esk)...")
since = (datetime.now(UTC) - timedelta(days=7)).strftime('%Y-%m-%d')

url = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"
params = {
    "service": "kisters",
    "type": "queryServices",
    "datasource": "0",
    "request": "getTimeseriesValues",
    "ts_path": "1/133148/SG/15m.Cmd",
    "from": since,
    "format": "json"
}

data = api_get(url, params)
count = 0

if data and isinstance(data, list):
    for item in data:
        if isinstance(item, dict) and 'data' in item and isinstance(item['data'], list):
            for entry in item['data']:
                if isinstance(entry, list) and len(entry) >= 2:
                    try:
                        ts = entry[0]
                        level = float(entry[1])
                        insert_reading("133148", "Esk", "Canonbie", level, ts)
                        count += 1
                    except:
                        pass

print(f"  → Inserted {count} readings for Canonbie")
print("Backfill complete")