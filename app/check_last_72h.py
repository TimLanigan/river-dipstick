#!/usr/bin/env python3
import os, psycopg2
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
conn = psycopg2.connect(f"postgresql://river_user:{DB_PASS}@wintermute-db/river_levels_db")
cur = conn.cursor()

cutoff = datetime.now(timezone.utc) - timedelta(hours=72)

print("\n=== LAST 72 HOURS — REAL DATA CHECK ===\n")
print(f"{'Station':<12} {'Label':<25} {'Readings':<10} {'Latest (UTC)':<20} {'Age'}")
print("-" * 75)

cur.execute("""
    SELECT 
        station_id,
        COALESCE(label, station_id) AS label,
        COUNT(*) AS cnt,
        MAX(timestamp::timestamptz) AS latest
    FROM readings
    WHERE timestamp >= %s
    GROUP BY station_id, label
    ORDER BY cnt DESC, latest DESC;
""", (cutoff,))

now = datetime.now(timezone.utc)

for sid, label, cnt, latest_raw in cur.fetchall():
    # Force latest to be timezone-aware UTC
    if latest_raw.tzinfo is None:
        latest = latest_raw.replace(tzinfo=timezone.utc)
    else:
        latest = latest_raw.astimezone(timezone.utc)

    age_min = int((now - latest).total_seconds() / 60)
    age = f"{age_min}m ago" if age_min < 1440 else f"{age_min//60}h ago"
    latest_str = latest.strftime("%Y-%m-%d %H:%M")

    print(f"{sid:<12} {str(label)[:24]:<25} {cnt:<10} {latest_str:<20} {age}")

print("\nExpected: ~288 real readings per station in 72 hours")
print("288 = perfect | 190–200 = still missing some | <100 = bad")
conn.close()