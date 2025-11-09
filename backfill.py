import requests
import sqlite3
from datetime import datetime, timedelta

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

def fetch_historical_readings(station_id, start_date, end_date):
    """Fetches readings for a date range."""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings?parameter=level&since={start_date}&_sorted"
    readings = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'items' in data:
            for item in data['items']:
                ts = item['dateTime']
                if ts < end_date:  # Filter to range (API 'since' is start only)
                    readings.append((item['value'], ts))
        return readings
    except requests.RequestException as e:
        print(f"Error fetching for {station_id}: {e}")
        return []

# All stations (from your baseline, excluding Penwortham if removed)
stations = {
    '710301': ('Ribble', 'Low Moor'),
    '713056': ('Ribble', 'New Jumbles Rock'),
    '713019': ('Ribble', 'Samlesbury'),
    '713030': ('Ribble', 'Walton-Le-Dale'),
    '713040': ('Ribble', 'Ribchester School'),
    '710305': ('Ribble', 'Henthorn'),
    '710102': ('Ribble', 'Penny Bridge'),
    '710151': ('Ribble', 'Locks Weir'),
    '710103': ('Ribble', 'Arnford Weir'),
    '760502': ('Eden', 'Temple Sowerby'),
    '760115': ('Eden', 'Appleby'),
    '762600': ('Eden', 'Sands Centre, Carlisle'),
    '762540': ('Eden', 'Linstock'),
    '762505': ('Eden', 'Great Corby'),
    '760101': ('Eden', 'Kirkby Stephen'),
    '765512': ('Eden', 'Sheepmount'),
    '760112': ('Eden', 'Great Musgrave Bridge')
}

# Backfill settings (adjust days_back as needed)
days_back = 365
end_date = datetime.utcnow().isoformat() + 'Z'
start_date_base = (datetime.utcnow() - timedelta(days=days_back)).isoformat() + 'Z'

for station_id, (river, label) in stations.items():
    print(f"Backfilling {station_id}...")
    current_start = datetime.fromisoformat(start_date_base[:-1])  # Remove Z for calc
    while current_start < datetime.utcnow():
        chunk_end = min(current_start + timedelta(days=30), datetime.utcnow())
        chunk_start_str = current_start.isoformat() + 'Z'
        chunk_end_str = chunk_end.isoformat() + 'Z'
        readings = fetch_historical_readings(station_id, chunk_start_str, chunk_end_str)
        for level, timestamp in readings:
            insert_reading(station_id, river, label, level, timestamp)
        current_start = chunk_end
    print(f"Completed backfill for {station_id}")