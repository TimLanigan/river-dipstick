#!/usr/bin/env python3
"""
backfill_gap.py - Backfills the 48-hour gap with real EA data (using original method)
"""
import requests
import psycopg2
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
import os
from river_reference import STATIONS

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

def api_get(url, params=None):
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"API error (attempt {attempt+1}): {e}")
            time.sleep(5)
    return None

def fetch_missing_readings(station_id, since_date):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    data = api_get(url, params={"parameter": "level", "since": since_date, "_sorted": ""})
    if data and 'items' in data:
        return [(item['value'], item['dateTime']) for item in data['items']]
    return []

conn = psycopg2.connect(CONNECTION_STRING)
cur = conn.cursor()

since = "2025-12-04T16:30:00Z"  # Outage start

total = 0

for river, stations in STATIONS.items():
    for station in stations:
        sid = station['id']
        label = station['label']
        print(f"Backfilling {label} ({sid})...")
        readings = fetch_missing_readings(sid, since)
        inserted = 0
        for level, ts in readings:
            cur.execute('''
                INSERT INTO readings (station_id, river, label, level, timestamp, good_level)
                VALUES (%s, %s, %s, %s, %s, 'n')
                ON CONFLICT (station_id, timestamp) DO NOTHING
            ''', (sid, river, label, level, ts))
            if cur.rowcount:
                inserted += 1
        if inserted:
            print(f"  Inserted {inserted} real readings")
            total += inserted
        conn.commit()

conn.close()
print(f"\nBackfill complete — inserted {total} real readings with correct river/label")