import requests
import sqlite3
from datetime import datetime, timedelta
import time

DB_FILE = '/home/river_levels_app/river_levels.db'

def insert_reading(station_id, river, label, level, timestamp):
    """Inserts a reading if it's not a duplicate (by timestamp and station)."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM readings WHERE station_id = ? AND timestamp = ?", (station_id, timestamp))
    if cursor.fetchone() is None:
        cursor.execute('''
            INSERT INTO readings (station_id, river, label, level, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (station_id, river, label, level, timestamp))
        conn.commit()
        print(f"Inserted reading for {station_id} at {timestamp}")
    else:
        print(f"Duplicate reading for {station_id} at {timestamp} skipped")
    conn.close()

def fetch_historical_readings(station_id, start_date):
    """Fetches readings since a start date (up to now)."""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings?parameter=level&since={start_date}&_sorted"
    print(f"Fetching URL: {url}")  # Debug
    readings = []
    try:
        response = requests.get(url)
        print(f"Response status: {response.status_code}")  # Debug
        if response.status_code != 200:
            print(f"Response text: {response.text}")  # Debug error
        response.raise_for_status()
        data = response.json()
        if 'items' in data:
            for item in data['items']:
                readings.append((item['value'], item['dateTime']))
        return readings
    except requests.RequestException as e:
        print(f"Error fetching for {station_id}: {e}")
        return []

# All stations (from your reference)
from river_reference import STATIONS as stations

# Backfill settings (API limit ~28 days)
chunk_days = 7
days_back = 14
now = datetime.utcnow()
start_date_base = now - timedelta(days=days_back)

for station_id, (river, label) in stations.items():
    print(f"Backfilling {station_id}...")
    current_start = start_date_base
    while current_start < now:
        chunk_end = min(current_start + timedelta(days=chunk_days), now)
        chunk_start_str = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
        readings = fetch_historical_readings(station_id, chunk_start_str)
        for level, timestamp in readings:
            insert_reading(station_id, river, label, level, timestamp)
        current_start = chunk_end
        time.sleep(1)  # Avoid rate limits
    print(f"Completed backfill for {station_id}")