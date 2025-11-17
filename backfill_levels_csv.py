#!/usr/bin/env python3
"""
backfill_levels_csv.py
Backfill level data from EA CSV archives (1+ year)
"""

import requests
import csv
from datetime import datetime, timedelta, UTC
import psycopg2
import time
from river_reference import STATIONS

DB_CONN = 'postgresql://river_user:***REMOVED***@localhost/river_levels_db'
DAYS_BACK = 365
CSV_URL_TEMPLATE = "https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date}.csv"

conn = psycopg2.connect(DB_CONN)
cur = conn.cursor()

def insert_reading(station_id, river, label, level, timestamp):
    cur.execute("""
        INSERT INTO readings (station_id, river, label, level, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (station_id, timestamp) DO NOTHING
    """, (station_id, river, label, level, timestamp))
    conn.commit()

# Target: Add river from stations.csv
target_river = 'River Hodder'
target_stations = {s['id']: s['label'] for s in STATIONS[target_river]}

print(f"Backfilling levels for {target_river} ({len(target_stations)} stations)")

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
            station_ref = row.get('stationReference', '')
            measure = row.get('measure', '')
            value = row.get('value', '')
            ts = row.get('dateTime', '')

            if not (station_ref and measure and value and ts):
                continue

            if station_ref not in target_stations:
                continue

            if 'level' not in measure:
                continue

            try:
                level = float(value)
                label = target_stations[station_ref]
                insert_reading(station_ref, target_river, label, level, ts)
                daily_inserts += 1
            except ValueError:
                continue

        print(f"  → {daily_inserts} level readings inserted")
    except Exception as e:
        print(f"  → Error: {e}")

    current_date += timedelta(days=1)
    time.sleep(1)

conn.close()
print("LEVEL CSV BACKFILL COMPLETE")