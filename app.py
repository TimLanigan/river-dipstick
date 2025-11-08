import requests
import sqlite3
from datetime import datetime

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

def insert_reading(station_id, river, label, level, timestamp):
    """Inserts a new reading if it's newer than the last one in DB."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Check latest timestamp for this station
    cursor.execute("SELECT MAX(timestamp) FROM readings WHERE station_id = ?", (station_id,))
    last_ts = cursor.fetchone()[0]
    if last_ts is None or timestamp > last_ts:
        cursor.execute('''
            INSERT INTO readings (station_id, river, label, level, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (station_id, river, label, level, timestamp))
        conn.commit()
        print(f"Inserted new reading for {station_id} at {timestamp}")
    else:
        print(f"Reading for {station_id} at {timestamp} already exists or older")
    conn.close()

# Initialize DB
init_db()

# All stations for River Ribble and River Eden
stations = {
    # River Ribble
    '710301': ('Ribble', 'Low Moor'),
    '713056': ('Ribble', 'New Jumbles Rock'),
    '713019': ('Ribble', 'Samlesbury'),
    '713030': ('Ribble', 'Walton-Le-Dale'),
    '713040': ('Ribble', 'Ribchester School'),
    '710305': ('Ribble', 'Henthorn'),
    '710102': ('Ribble', 'Penny Bridge'),
    '710151': ('Ribble', 'Locks Weir'),
    '710103': ('Ribble', 'Arnford Weir'),
    '713354': ('Ribble', 'Penwortham'),
    
    # River Eden
    '760502': ('Eden', 'Temple Sowerby'),
    '760115': ('Eden', 'Appleby'),
    '762600': ('Eden', 'Sands Centre, Carlisle'),
    '762540': ('Eden', 'Linstock'),
    '762505': ('Eden', 'Great Corby'),
    '760101': ('Eden', 'Kirkby Stephen'),
    '765512': ('Eden', 'Sheepmount'),
    '760112': ('Eden', 'Great Musgrave Bridge')
}

# Fetch and store data
print(f"Collecting River Level Data as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
for station_id, (river, label) in stations.items():
    level, timestamp = get_latest_river_level(station_id)
    if level is not None:
        insert_reading(station_id, river, label, level, timestamp)
    else:
        print(f"{river} - {label} ({station_id}): No recent data available")

