#!/usr/bin/env python3
"""
backfill_rain.py - Simple recent rainfall backfill
"""
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sys
import time
from io import StringIO

sys.path.append("/app")
from river_reference import STATIONS

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONN = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

def get_conn():
    return psycopg2.connect(CONN)

def insert_rainfall(level_id, rain_id, df):
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
        """, (level_id, rain_id, row['timestamp'], row['rainfall_mm']))
        if cur.rowcount > 0:
            inserted += 1
    conn.commit()
    conn.close()
    return inserted

if __name__ == "__main__":
    print("=== Rainfall Backfill - Last 7 Days ===")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    # Build mapping
    rain_to_level = {}
    for river, stations in STATIONS.items():
        for s in stations:
            rid = s.get('rainfall_id')
            if rid:
                rain_to_level[rid] = s['id']

    total = 0
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        url = f"https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date_str}.csv"
        
        print(f"Checking {date_str}...")
        
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"  No data for {date_str}")
            current += timedelta(days=1)
            continue
            
        df = pd.read_csv(StringIO(resp.text), low_memory=False)
        rain_df = df[df['stationReference'].isin(rain_to_level.keys())].copy()
        
        if not rain_df.empty:
            rain_df = rain_df[['stationReference', 'dateTime', 'value']].rename(
                columns={'dateTime': 'timestamp', 'value': 'rainfall_mm'}
            )
            rain_df['timestamp'] = pd.to_datetime(rain_df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            daily = 0
            for rid, group in rain_df.groupby('stationReference'):
                level_id = rain_to_level.get(rid)
                if level_id:
                    inserted = insert_rainfall(level_id, rid, group)
                    daily += inserted
            print(f"  Inserted {daily} readings for {date_str}")
            total += daily
        else:
            print(f"  No relevant rainfall data on {date_str}")
            
        current += timedelta(days=1)
        time.sleep(1)

    print(f"\nBackfill finished. Total inserted: {total}")