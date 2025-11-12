import requests
import csv
from datetime import datetime, timedelta
import sqlite3
import time


DB_FILE = '/home/river_levels_app/river_levels.db'

def insert_reading(cursor, station_id, river, label, level, timestamp):
    cursor.execute("SELECT 1 FROM readings WHERE station_id = ? AND timestamp = ?", (station_id, timestamp))
    if cursor.fetchone() is None:
        cursor.execute('''
            INSERT INTO readings (station_id, river, label, level, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (station_id, river, label, level, timestamp))
        print(f"Inserted reading for {station_id} at {timestamp}")
    else:
        print(f"Duplicate reading for {station_id} at {timestamp} skipped")

def get_earliest_date(cursor, station_id):
    cursor.execute("SELECT MIN(timestamp) FROM readings WHERE station_id = ?", (station_id,))
    result = cursor.fetchone()[0]
    if result:
        # Parse the timestamp (assuming ISO with Z)
        return datetime.fromisoformat(result.replace('Z', '+00:00'))
    else:
        print(f"No existing data for {station_id} - skipping backfill")
        return None

# Stations from reference
from river_reference import STATIONS  # Updated to use STATIONS for grouped structure

# Backfill settings (days to backfill before earliest date per station)
days_back = 1
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

for river, station_list in STATIONS.items():
    for station in station_list:
        station_id = station['id']
        label = station['label']
        earliest = get_earliest_date(cursor, station_id)
        if earliest is None:
            continue  # Skip if no data
        start_date = earliest - timedelta(days=days_back)
        end_date = earliest  # Backfill up to but not including existing earliest

        print(f"Backfilling {station_id} ({river} - {label}) from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

        current_date = start_date
        while current_date < end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            url = f"https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date_str}.csv"
            print(f"Downloading and processing CSV for {date_str} (URL: {url})...")
            try:
                response = requests.get(url, stream=True)
                print(f"Response status: {response.status_code}")
                if response.status_code == 200:
                    row_count = 0
                    match_count = 0
                    first_match_printed = False
                    reader = csv.DictReader(line.decode('utf-8') for line in response.iter_lines())
                    for row in reader:
                        row_count += 1
                        station_ref = row.get('stationReference', '')
                        value_str = row.get('value', '')
                        timestamp = row.get('dateTime', '')
                        if value_str and timestamp and station_ref == station_id:  # Exact match on stationReference
                            match_count += 1
                            if not first_match_printed:
                                print(f"First match row for {station_id}: {row}")
                                first_match_printed = True
                            try:
                                level = float(value_str)
                                insert_reading(cursor, station_id, river, label, level, timestamp)
                            except ValueError:
                                print(f"Invalid level '{value_str}' for {station_id} at {timestamp}")
                    print(f"Processed {row_count} rows for {date_str}, found {match_count} matches")
                    conn.commit()  # Commit after each CSV
                else:
                    print(f"Response text: {response.text if response.text else 'No text'}")
            except Exception as e:
                print(f"Error for {date_str}: {e}")
            current_date += timedelta(days=1)
            time.sleep(1)  # Rate limit

conn.close()
print("Backfill complete.")
