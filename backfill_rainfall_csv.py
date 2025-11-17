#!/usr/bin/env python3
"""
backfill_rainfall_csv.py — FINAL
Backfill rainfall from EA CSV archives (1+ year)
"""
import requests
import csv
from datetime import datetime, timedelta, UTC
import psycopg2
import time
from river_reference import STATIONS

# === CONFIG ===
DB_CONN = 'postgresql://river_user:***REMOVED***@localhost/river_levels_db'
DAYS_BACK = 10
CSV_URL_TEMPLATE = "https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date}.csv"

conn = psycopg2.connect(DB_CONN)
cur = conn.cursor()

def insert_rainfall(level_station_id, rainfall_id, rainfall_mm, timestamp):
    cur.execute("""
        INSERT INTO rainfall_readings (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (level_station_id, timestamp) DO NOTHING
    """, (level_station_id, rainfall_id, rainfall_mm, timestamp))
    conn.commit()

# === BUILD MEASURE URL MAP (ALL VARIANTS) ===
rainfall_measure_to_level = {}
for river, stations in STATIONS.items():
    for s in stations:
        if 'rainfall_id' in s:
            base = f"{s['rainfall_id']}-rainfall-tipping_bucket-raingauge-i-15_min-mm"
            variants = [
                f"http://environment.data.gov.uk/flood-monitoring/id/measures/{base}",
                f"https://environment.data.gov.uk/flood-monitoring/id/measures/{base}",
                f"http://data.gov.uk/flood-monitoring/id/measures/{base}",
                f"https://data.gov.uk/flood-monitoring/id/measures/{base}",
            ]
            for v in variants:
                rainfall_measure_to_level[v] = s['id']

print(f"Found {len(rainfall_measure_to_level)} measure variants")

# === BACKFILL ===
end_date = datetime.now(UTC).date()
start_date = end_date - timedelta(days=DAYS_BACK)
current_date = start_date

total_inserted = 0
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
            if measure not in rainfall_measure_to_level:
                continue
            level_station_id = rainfall_measure_to_level[measure]
            rainfall_id = measure.split('/')[-1].split('-')[0]
            value = row.get('value')
            ts = row.get('dateTime')
            if not (value and ts):
                continue
            try:
                rain_mm = float(value)
                insert_rainfall(level_station_id, rainfall_id, rain_mm, ts)
                daily_inserts += 1
            except ValueError:
                continue

        print(f"  → {daily_inserts} inserted")
        total_inserted += daily_inserts
    except Exception as e:
        print(f"  → Error: {e}")

    current_date += timedelta(days=1)
    time.sleep(1)

conn.close()
print(f"RAINFALL BACKFILL COMPLETE: {total_inserted} total readings")