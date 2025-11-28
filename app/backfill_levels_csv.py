#!/usr/bin/env python3
"""
backfill_levels_missing.py
Smart backfill — only fills gaps in the last 365 days
Runs manually or daily at 3am — the ultimate safety net
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
DAYS_BACK = 365

def get_missing_dates(conn):
    """Find dates with no readings for any station in last 365 days"""
    cur = conn.cursor()
    query = """
    SELECT date_trunc('day', generate_series(
        now()::date - interval '365 days',
        now()::date,
        '1 day'
    ))::date AS missing_date
    EXCEPT
    SELECT DISTINCT date_trunc('day', timestamp)::date FROM readings
    ORDER BY missing_date;
    """
    cur.execute(query)
    dates = [row[0] for row in cur.fetchall()]
    cur.close()
    return dates

def insert_reading(cur, station_id, river, label, level, timestamp_str):
    ts = timestamp_str.replace('Z', '+00')
    cur.execute("""
        INSERT INTO readings (station_id, river, label, level, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (station_id, timestamp) DO NOTHING
    """, (station_id, river, label, level, ts))

def main():
    print(f"BACKFILL_MISSING — checking last {DAYS_BACK} days for gaps")
    conn = psycopg2.connect(CONNECTION_STRING)
    conn.autocommit = False
    cur = conn.cursor()

    all_stations = {s['id']: (river, s['label']) for river in STATIONS for s in STATIONS[river]}

    missing_dates = get_missing_dates(conn)
    if not missing_dates:
        print("No gaps found — database is complete!")
        conn.close()
        return

    print(f"Found {len(missing_dates)} missing day(s): {missing_dates[0]} → {missing_dates[-1]}")

    total_inserted = 0
    for date in missing_dates:
        date_str = date.strftime('%Y-%m-%d')
        url = CSV_URL_TEMPLATE.format(date=date_str)
        print(f"[{date_str}] Fetching...", end="")

        for attempt in range(3):
            try:
                response = requests.get(url, stream=True, timeout=30)
                if response.status_code == 404:
                    print(" → No data (weekend?)")
                    break
                if response.status_code != 200:
                    print(f" → HTTP {response.status_code}")
                    break

                daily = 0
                reader = csv.DictReader(
                    line.decode('utf-8', errors='ignore')
                    for line in response.iter_lines() if line
                )
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
                    insert_reading(cur, ref, river, label, level, ts)
                    daily += 1
                    total_inserted += 1

                print(f" → {daily} readings")
                conn.commit()
                time.sleep(0.8)
                break
            except Exception as e:
                if attempt < 2:
                    print(f" retry {attempt+1}")
                    time.sleep(5)
                else:
                    print(f" → FAILED: {e}")
        else:
            print(" → Skipped")

    conn.close()
    print(f"\nBACKFILL COMPLETE — {total_inserted:,} missing readings added")
    print("THE BEAST IS FED AND WHOLE.")

if __name__ == "__main__":
    main()