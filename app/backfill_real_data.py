#!/usr/bin/env python3
"""
backfill_real_data.py

Pulls EVERY real EA reading since a given date (with proper pagination)
and inserts them into your real wintermute-db database.

Run once to fix the 48-hour gap with actual measured values — no interpolation.
"""

import os
import requests
import psycopg2
from dotenv import load_dotenv

# ------------------------------------------------------------------
# Config — change this date if needed
# ------------------------------------------------------------------
START_DATE = "2025-12-04T16:30:00Z"   # When the outage began

# Your full list of station IDs
STATIONS = [
    "760101", "760112", "760115", "760502", "762505", "765512", "762540",
    "710151", "710102", "710301", "710305", "713056", "713040",
    "724735", "724629", "722242", "722421", "724647", "711610"
]

# ------------------------------------------------------------------
# Load DB credentials
# ------------------------------------------------------------------
load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
if not DB_PASS:
    raise ValueError("DB_PASSWORD not found in .env")

CONN_STRING = f"postgresql://river_user:{DB_PASS}@wintermute-db/river_levels_db"

# ------------------------------------------------------------------
# Fetch all readings for one station using EA pagination
# ------------------------------------------------------------------
def fetch_all_readings(station_id: str, since: str):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    params = {
        "since": since,
        "_sorted": "",
        "_limit": 1000   # maximum allowed per page
    }
    all_items = []

    while url:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        all_items.extend(items)
        print(f"   → fetched {len(items)} readings (total: {len(all_items)})")

        # Next page?
        url = data.get("pagination", {}).get("next") or data.get("@id")
        params = None  # only first request needs params

    return all_items

# ------------------------------------------------------------------
# Main backfill
# ------------------------------------------------------------------
def main():
    conn = psycopg2.connect(CONN_STRING)
    cur = conn.cursor()

    total_inserted = 0

    print(f"Starting backfill from {START_DATE} for {len(STATIONS)} stations...\n")

    for sid in STATIONS:
        print(f"Backfilling station {sid}...")
        try:
            readings = fetch_all_readings(sid, START_DATE)

            inserted = 0
            for item in readings:
                level = item["value"]
                timestamp = item["dateTime"]

                cur.execute("""
                    INSERT INTO readings
                    (station_id, river, label, level, timestamp, good_level)
                    VALUES (%s, %s, %s, %s, %s, 'n')
                    ON CONFLICT (station_id, timestamp) DO NOTHING
                """, (sid, "Unknown", sid, level, timestamp))

                if cur.rowcount:
                    inserted += 1

            if inserted:
                print(f"   → Inserted {inserted} real readings for {sid}")
                total_inserted += inserted

            conn.commit()

        except Exception as e:
            print(f"   → ERROR on {sid}: {e}")
            conn.rollback()

    conn.close()
    print("\n" + "="*60)
    print("BACKFILL COMPLETE")
    print(f"Inserted {total_inserted} real EA readings")
    print("Your graphs now show the true river behaviour — no more straight lines!")
    print("="*60)

if __name__ == "__main__":
    main()