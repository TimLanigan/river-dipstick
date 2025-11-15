#!/usr/bin/env python3
"""
backfill_rainfall_csv.py — FIXED
Backfill rainfall from EA CSV archives using measure URL
"""

import requests
import csv
from datetime import datetime, timedelta, UTC
import psycopg2
import time
from river_reference import STATIONS

# === CONFIG ===
DB_CONN = 'postgresql://river_user:***REMOVED***@localhost/river_levels_db'
DAYS_BACK = 365
CSV_URL_TEMPLATE = "https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date}.csv"

# === DB ===
conn = psycopg2.connect(DB_CONN)
cur = conn.cursor()

def insert_rainfall(level_station_id, rainfall_id, rainfall_mm, timestamp):
    cur.execute("""
        INSERT INTO rainfall_readings (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (level_station_id, timestamp) DO NOTHING
    """, (level_station_id, rainfall_id, rainfall_mm, timestamp))
    conn.commit()

# === BUILD RAINFALL MEASURE URL MAP ===
rainfall_measure_to_level = {}
for river, stations in STATIONS.items():
    for s in stations:
        if 'rainfall_id' in s:
            # Build expected measure URL
            measure_url = f"http://environment.data.gov.uk/flood-monitoring/id/measures/{s['rainfall_id']}-rainfall-tipping_bucket-raingauge-i-15_min-mm"
            rainfall_measure_to_level[measure_url] = s['id']

print(f"Found {len(rainfall_measure_to_level)} rainfall measures")

# === BACKFILL ===
end_date = datetime.now(UTC).date()
start_date = end_date - timedelta(days=DAYS_BACK)
current_date = start_date

while current_date <= end_date:
    date_str = current_date.strftime('%Y-%m-%d')
    url = CSV_URL_TEMPLATE.format(date=date_str)
    print(f"Fetching {date_str}...")

    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code != 200:
            print(f"  → HTTP {response.status_code}")
            current_date += timedelta(days=1)
            time.sleep(1)
            continue

        reader = csv.DictReader(line.decode('utf-8') for line in response.iter_lines())
        daily_inserts = 0
        for row in reader:
            measure = row.get('measure', '')
            value = row.get('value', '')
            ts = row.get('dateTime', '')

            if not (measure and value and ts):
                continue

            if measure not in rainfall_measure_to_level:
                continue

            try:
                rain_mm = float(value)
                level_station_id = rainfall_measure_to_level[measure]
                insert_rainfall(level_station_id, measure.split('/')[-1].split('-')[0], rain_mm, ts)
                daily_inserts += 1
            except (ValueError, IndexError):
                continue

        print(f"  → {daily_inserts} rain readings inserted")
    except Exception as e:
        print(f"  → Error: {e}")

    current_date += timedelta(days=1)
    time.sleep(1)

conn.close()
print("RAINFALL CSV BACKFILL COMPLETE")