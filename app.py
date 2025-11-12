import requests
import sqlite3
from datetime import datetime, timedelta, timezone, UTC
import time
from river_reference import STATIONS


DB_FILE = '/home/river_levels_app/river_levels.db'

def init_db():
    """Initialize the SQLite database and create table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            river TEXT NOT NULL,
            label TEXT NOT NULL,
            level REAL,
            timestamp TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def get_latest_river_level(station_id):
    """Fetches the latest river level (stage) for a given station ID."""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings?latest&parameter=level"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'items' in data and data['items']:
            reading = data['items'][0]
            return reading['value'], reading['dateTime']
        return None, None
    except requests.RequestException as e:
        print(f"Error fetching data for {station_id}: {e}")
        return None, None

def fetch_missing_readings(station_id, since_date):
    """Fetches readings since a given date to fill gaps."""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings?parameter=level&since={since_date}&_sorted"
    readings = []
    try:
        response = requests.get(url)
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
    """Inserts a new reading if it's newer than the last one in DB or fills gap."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Check if this exact timestamp exists (to avoid duplicates)
    cursor.execute("SELECT 1 FROM readings WHERE station_id = ? AND timestamp = ?", (station_id, timestamp))
    if cursor.fetchone() is None:
        cursor.execute('''
            INSERT INTO readings (station_id, river, label, level, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (station_id, river, label, level, timestamp))
        conn.commit()
        print(f"Inserted reading for {station_id} at {timestamp}")
    else:
        print(f"Reading for {station_id} at {timestamp} already exists")
    conn.close()

def check_gaps(station_id, days=2):
    """Checks if there are gaps in the last N days (e.g., < expected readings)."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    since_date = (datetime.now(UTC) - timedelta(days=days)).isoformat() + 'Z'
    cursor.execute("SELECT COUNT(*) FROM readings WHERE station_id = ? AND timestamp >= ?", (station_id, since_date))
    count = cursor.fetchone()[0]
    conn.close()
    expected = days * 24 * 4  # Approx 15-min intervals (96 per day)
    return count < expected * 0.9  # If <90% expected, consider gapped

# Initialize DB
init_db()

# Fetch and store data
print(f"Collecting River Level Data as of {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}")
for river, station_list in STATIONS.items():
    for station in station_list:
        station_id = station['id']
        label = station['label']
        level, timestamp = get_latest_river_level(station_id)
        if level is not None:
            insert_reading(station_id, river, label, level, timestamp)
        else:
            print(f"{river} - {label} ({station_id}): No recent data available")

        # Always check for gaps in last 2 days and backfill if needed
        if check_gaps(station_id):
            print(f"Gaps detected for {station_id} - backfilling last 2 days")
            since_date = (datetime.now(UTC) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
            missing_readings = fetch_missing_readings(station_id, since_date)
            for m_level, m_timestamp in missing_readings:
                insert_reading(station_id, river, label, m_level, m_timestamp)
        time.sleep(1)  # Short delay to avoid rate limiting