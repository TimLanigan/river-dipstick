#!/usr/bin/env python3
"""
backfill_rain_archive.py – Backfill rainfall from EA daily archive CSVs (last 12 months)
Downloads daily full CSV if exists, filters rainfall for your stations, inserts safely
Checks availability, fills both level and rainfall station IDs
"""
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time
import sys
from pathlib import Path
from io import StringIO

# Add /app to path for river_reference import
sys.path.append("/app")
from river_reference import STATIONS

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONN = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'
ARCHIVE_BASE = "https://environment.data.gov.uk/flood-monitoring/archive"

def get_conn():
    return psycopg2.connect(CONN)

def insert_rainfall(level_station_id, rainfall_station_id, df):
    if df.empty:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO rainfall_readings (level_station_id, rainfall_station_id, timestamp, rainfall_mm)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (level_station_id, timestamp) DO NOTHING
        """, (level_station_id, rainfall_station_id, row['timestamp'], row['rainfall_mm']))
        if cur.rowcount:
            inserted += 1
    conn.commit()
    conn.close()
    return inserted

if __name__ == "__main__":
    print("Starting rainfall backfill from daily archive CSVs (last 12 months)...")
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()
    current_date = start_date
    total_inserted = 0
    rainfall_ids = set()
    id_to_level = {}
    for river, stations in STATIONS.items():
        for station in stations:
            rid = station.get('rainfall_id')
            if rid:
                rainfall_ids.add(rid)
                id_to_level[rid] = station['id']
    if not rainfall_ids:
        print("No rainfall_ids in STATIONS – exiting")
        exit()
    print(f"Found {len(rainfall_ids)} rainfall_ids to backfill")
    while current_date < end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        url = f"{ARCHIVE_BASE}/readings-full-{date_str}.csv"
        print(f"Checking {url}...")
        head_resp = requests.head(url, timeout=10)
        if head_resp.status_code != 200:
            print(f"  No archive for {date_str} (status {head_resp.status_code})")
            current_date += timedelta(days=1)
            continue
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"  Download failed for {date_str}")
            current_date += timedelta(days=1)
            continue
        df = pd.read_csv(StringIO(resp.text))
        rain_df = df[df['stationReference'].isin(rainfall_ids)]
        if rain_df.empty:
            print(f"  No rainfall data for our stations on {date_str}")
        else:
            daily_inserted = 0
            for rid, group in rain_df.groupby('stationReference'):
                level_id = id_to_level.get(rid)
                if not level_id:
                    continue
                group = group[['dateTime', 'value']].rename(columns={'dateTime': 'timestamp', 'value': 'rainfall_mm'})
                group['timestamp'] = pd.to_datetime(group['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%S%z')
                inserted = insert_rainfall(level_id, rid, group)
                daily_inserted += inserted
            print(f"  Inserted {daily_inserted} new rainfall readings from {len(rain_df)} rows")
            total_inserted += daily_inserted
        time.sleep(1)
        current_date += timedelta(days=1)
    print(f"Backfill complete — total new rainfall readings inserted: {total_inserted}")
    print("Rerun predictor for improved forecasts.")