#!/usr/bin/env python3
"""
get_readings.py - Collect river levels + rainfall every 15 mins
Updated: 2025-11-15 15:28 CET by @Moldfingers + Grok
"""

import requests
import psycopg2
from datetime import datetime, timedelta, UTC
import time
from river_reference import STATIONS  # Now includes lat/lon/rainfall_id

CONNECTION_STRING = "dbname=river_levels_db user=river_user password=***REMOVED*** host=localhost"

# --------------------------------------------------------------------------- #
# DATABASE SETUP
# --------------------------------------------------------------------------- #
def init_db():
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    # River levels table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS readings (
            id SERIAL PRIMARY KEY,
            station_id TEXT NOT NULL,
            river TEXT NOT NULL,
            label TEXT NOT NULL,
            level REAL,
            timestamp TEXT NOT NULL
        )
    ''')
    # Rainfall table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rainfall_readings (
            id SERIAL PRIMARY KEY,
            level_station_id TEXT NOT NULL,
            rainfall_station_id TEXT NOT NULL,
            rainfall_mm REAL,
            timestamp TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# --------------------------------------------------------------------------- #
# RIVER LEVELS
# --------------------------------------------------------------------------- #
def get_latest_river_level(station_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings?latest&parameter=level"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if 'items' in data and data['items']:
            reading = data['items'][0]
            return reading['value'], reading['dateTime']
        return None, None
    except requests.RequestException as e:
        print(f"Error fetching level for {station_id}: {e}")
        return None, None

def fetch_missing_readings(station_id, since_date):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings?parameter=level&since={since_date}&_sorted"
    readings = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if 'items' in data:
            for item in data['items']:
                readings.append((item['value'], item['dateTime']))
        return readings
    except requests.RequestException as e:
        print(f"Error fetching missing data for {station_id}: {e}")
        return []

def insert_reading(station_id, river, label, level, timestamp):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM readings WHERE station_id = %s AND timestamp = %s", (station_id, timestamp))
    if cursor.fetchone() is None:
        cursor.execute('''
            INSERT INTO readings (station_id, river, label, level, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        ''', (station_id, river, label, level, timestamp))
        conn.commit()
        print(f"Inserted level {level}m for {label} ({station_id}) at {timestamp}")
    else:
        print(f"Level for {station_id} at {timestamp} already exists")
    conn.close()

# --------------------------------------------------------------------------- #
# RAINFALL
# --------------------------------------------------------------------------- #
def get_latest_rainfall(rainfall_station_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{rainfall_station_id}/readings?latest&parameter=rainfall"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if 'items' in data and data['items']:
            reading = data['items'][0]
            return reading['value'], reading['dateTime']
        return None, None
    except requests.RequestException as e:
        print(f"Error fetching rainfall for {rainfall_station_id}: {e}")
        return None, None

def fetch_missing_rainfall(rainfall_id, since_date):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{rainfall_id}/readings?parameter=rainfall&since={since_date}&_sorted"
    readings = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if 'items' in data:
            for item in data['items']:
                readings.append((item['value'], item['dateTime']))
        return readings
    except requests.RequestException as e:
        print(f"Error fetching missing rainfall for {rainfall_id}: {e}")
        return []

def insert_rainfall(level_station_id, rainfall_station_id, rainfall_mm, timestamp):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM rainfall_readings WHERE level_station_id = %s AND rainfall_station_id = %s AND timestamp = %s",
                   (level_station_id, rainfall_station_id, timestamp))
    if cursor.fetchone() is None:
        cursor.execute('''
            INSERT INTO rainfall_readings (level_station_id, rainfall_station_id, rainfall_mm, timestamp)
            VALUES (%s, %s, %s, %s)
        ''', (level_station_id, rainfall_station_id, rainfall_mm, timestamp))
        conn.commit()
        print(f"Inserted rainfall {rainfall_mm}mm for {level_station_id} from {rainfall_station_id} at {timestamp}")
    else:
        print(f"Rainfall for {level_station_id} at {timestamp} already exists")
    conn.close()

# --------------------------------------------------------------------------- #
# GAPS
# --------------------------------------------------------------------------- #
def check_gaps(station_id, days=2):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat() + 'Z'
    cursor.execute("SELECT COUNT(*) FROM readings WHERE station_id = %s AND timestamp >= %s", (station_id, since_date))
    count = cursor.fetchone()[0]
    conn.close()
    expected = days * 24 * 4  # 15-min intervals
    return count < expected * 0.9

# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    init_db()
    print(f"Collecting River Level + Rainfall Data as of {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")

    for river, station_list in STATIONS.items():
        for station in station_list:
            station_id = station['id']
            label = station['label']
            rainfall_id = station.get('rainfall_id')

            # === RIVER LEVEL ===
            level, timestamp = get_latest_river_level(station_id)
            if level is not None:
                insert_reading(station_id, river, label, level, timestamp)
            else:
                print(f"{river} - {label} ({station_id}): No recent level data")

            # === RAINFALL (ENHANCED WITH DEBUG + HOURLY BACKFILL) ===
            if rainfall_id:
                rainfall_mm, rain_timestamp = get_latest_rainfall(rainfall_id)
                if rainfall_mm is not None:
                    aligned_ts = timestamp if timestamp else rain_timestamp
                    insert_rainfall(station_id, rainfall_id, rainfall_mm, aligned_ts)
                else:
                    print(f"{river} - {label} ({station_id}): No recent rainfall from {rainfall_id} (may be hourly or offline)")
                    # Try hourly backfill (last 6 hours)
                    hourly_since = (datetime.now(UTC) - timedelta(hours=6)).strftime('%Y-%m-%dT%H:00:00Z')
                    hourly_rain = fetch_missing_rainfall(rainfall_id, hourly_since)
                    if hourly_rain:
                        latest_hour = hourly_rain[-1]  # Most recent
                        insert_rainfall(station_id, rainfall_id, latest_hour[0], latest_hour[1])
                        print(f"  → Backfilled hourly rain: {latest_hour[0]}mm at {latest_hour[1]}")

                        # === BACKFILL GAPS + RAINFALL (7 DAYS ON FIRST RUN) ===
            if check_gaps(station_id):
                print(f"Gaps detected for {station_id} - backfilling last 2 days")
                since_date = (datetime.now(UTC) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
                
                # Level backfill
                missing_levels = fetch_missing_readings(station_id, since_date)
                for m_level, m_ts in missing_levels:
                    insert_reading(station_id, river, label, m_level, m_ts)

                # Rainfall backfill (always 7 days)
                if rainfall_id:
                    rain_since = (datetime.now(UTC) - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')
                    missing_rain = fetch_missing_rainfall(rainfall_id, rain_since)
                    for m_rain, m_ts in missing_rain:
                        insert_rainfall(station_id, rainfall_id, m_rain, m_ts)
                    print(f"  → Backfilled {len(missing_rain)} rain readings (7 days) from {rainfall_id}")
            else:
                # First run: no gaps, but still backfill 7 days of rain
                if rainfall_id:
                    rain_since = (datetime.now(UTC) - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')
                    missing_rain = fetch_missing_rainfall(rainfall_id, rain_since)
                    existing_count = 0
                    for m_rain, m_ts in missing_rain:
                        # Skip if already exists
                        conn = psycopg2.connect(CONNECTION_STRING)
                        cur = conn.cursor()
                        cur.execute("SELECT 1 FROM rainfall_readings WHERE level_station_id = %s AND timestamp = %s", (station_id, m_ts))
                        if cur.fetchone() is None:
                            insert_rainfall(station_id, rainfall_id, m_rain, m_ts)
                        else:
                            existing_count += 1
                        cur.close()
                        conn.close()
                    print(f"  → Backfilled {len(missing_rain) - existing_count} new rain readings (7 days) from {rainfall_id}")

            time.sleep(1)  # Be kind to EA API

# === SMART RAINFALL BACKFILL TO MATCH LEVEL HISTORY ===
def backfill_rainfall_to_level_history():
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()

    # Get earliest level timestamp per station
    cursor.execute("""
        SELECT station_id, MIN(timestamp) 
        FROM readings 
        GROUP BY station_id
    """)
    stations = cursor.fetchall()

    total_backfilled = 0
    for station_id, earliest_ts_str in stations:
        # Convert string → datetime
        earliest_ts = datetime.fromisoformat(earliest_ts_str.replace('Z', '+00:00'))

        # Find matching rainfall_id from STATIONS
        rainfall_id = None
        for river, station_list in STATIONS.items():
            for s in station_list:
                if s['id'] == station_id:
                    rainfall_id = s.get('rainfall_id')
                    break
            if rainfall_id:
                break

        if not rainfall_id:
            print(f"No rainfall_id for {station_id} — skipping")
            continue

        print(f"Backfilling rainfall for {station_id} (rain gauge {rainfall_id}) from {earliest_ts}")

        # Batch by 30 days
        start = earliest_ts
        end = datetime.now(UTC)
        batch_size = timedelta(days=30)

        while start < end:
            batch_end = min(start + batch_size, end)
            since_str = start.strftime('%Y-%m-%dT%H:%M:%SZ')
            until_str = batch_end.strftime('%Y-%m-%dT%H:%M:%SZ')

            try:
                rain_data = fetch_missing_rainfall(rainfall_id, since_str)
                inserted = 0
                for rain_mm, ts in rain_data:
                    # Skip if already exists
                    cursor.execute(
                        "SELECT 1 FROM rainfall_readings WHERE level_station_id = %s AND timestamp = %s",
                        (station_id, ts)
                    )
                    if cursor.fetchone() is None:
                        insert_rainfall(station_id, rainfall_id, rain_mm, ts)
                        inserted += 1
                total_backfilled += inserted
                print(f"  → {inserted} new rain readings ({start.date()} to {batch_end.date()})")
            except Exception as e:
                print(f"  → Error fetching batch: {e}")

            start = batch_end
            time.sleep(5)  # Be kind to EA API

    conn.close()
    print(f"RAIN BACKFILL COMPLETE: {total_backfilled} total new readings")

# === RUN ONCE (UNCOMMENT TO EXECUTE) ===
# backfill_rainfall_to_level_history()
backfill_rainfall_to_level_history()