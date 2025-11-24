#!/usr/bin/env python3
"""
backfill_levels_one_year.py
GO HARD OR GO HOME — 365 days of every reading for every station
"""
import requests
import csv
from datetime import datetime, timedelta, UTC
import psycopg2
import time
import sys
from river_reference import STATIONS
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db:5432/river_levels_db'

CSV_URL_TEMPLATE = "https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date}.csv"

# FULL YEAR — 365 days
DAYS_BACK = 365

conn = psycopg2.connect(CONNECTION_STRING)
cur = conn.cursor()

def insert_reading(station_id, river, label, level, timestamp_str):
    ts = timestamp_str.replace('Z', '+00')
    cur.execute("""
        INSERT INTO readings (station_id, river, label, level, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (station_id, timestamp) DO NOTHING
    """, (station_id, river, label, level, ts))

total_inserted = 0
all_stations = {s['id']: (river, s['label']) for river in STATIONS for s in STATIONS[river]}

end_date = datetime.now(UTC).date()
start_date = end_date - timedelta(days=DAYS_BACK)
current_date = start_date

print(f"BACKFILLING ONE FULL YEAR ({DAYS_BACK} days) FOR ALL RIVERS — THIS IS WAR")

for day in range(DAYS_BACK + 1):
    current_date = start_date + timedelta(days=day)
    date_str = current_date.strftime('%Y-%m-%d')
    url = CSV_URL_TEMPLATE.format(date=date_str)
    
    print(f"[{day+1}/{DAYS_BACK+1}] Fetching {date_str}...", end="")
    
    for attempt in range(3):
        try:
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 404:
                print(" → No data (weekend/holiday?)")
                break
            if response.status_code != 200:
                print(f" → HTTP {response.status_code}")
                break

            daily = 0
            reader = csv.DictReader(line.decode('utf-8', errors='ignore') for line in response.iter_lines() if line)
            for row in reader:
                ref = row.get('stationReference', '').strip()
                if ref not in all_stations:
                    continue
                if 'level' not in row.get('measure', '').lower():
                    continue
                try:
                    level = float(row.get('value', '').strip())
                except:
                    continue
                ts = row.get('dateTime', '').strip()
                if not ts:
                    continue
                
                river, label = all_stations[ref]
                insert_reading(ref, river, label, level, ts)
                daily += 1
                total_inserted += 1

            print(f" → {daily} readings")
            break
        except Exception as e:
            if attempt < 2:
                print(f" retry {attempt+1}")
                time.sleep(5)
                continue
            else:
                print(f" → FAILED: {e}")
                break
    
    conn.commit()
    time.sleep(0.8)  # Be excellent to EA

conn.close()
print(f"\nONE YEAR BACKFILL COMPLETE — {total_inserted:,} TOTAL READINGS INGESTED")
print("THE BEAST IS FED.")
print("PROPHET WILL NOW SPEAK TRUTH.")