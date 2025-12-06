#!/usr/bin/env python3
"""
assess_db_status.py - The one that actually runs (tested 2025-12-02)
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
if not DB_PASS:
    print("DB_PASSWORD not found in .env")
    exit(1)

CONN = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'
engine = create_engine(CONN)

query = text("""
SELECT 
    r.station_id,
    r.label,
    COUNT(*) AS total_readings,
    MAX(r.timestamp::timestamptz AT TIME ZONE 'UTC') AS latest_utc,
    COUNT(*) FILTER (
        WHERE r.timestamp::timestamptz AT TIME ZONE 'UTC' >= 
              NOW() AT TIME ZONE 'UTC' - INTERVAL '24 hours'
    ) AS last_24h_count
FROM readings r
GROUP BY r.station_id, r.label
ORDER BY latest_utc DESC NULLS LAST;
""")

with engine.connect() as conn:
    rows = list(conn.execute(query).mappings())

print("\n" + "="*90)
print("RIVERDIPSTICK DEV - DATABASE HEALTH CHECK (UTC)")
print("="*90)
print(f"{'Station':<12} {'Label':<28} {'Latest (UTC)':<20} {'Age':<14} {'24h':<6} {'Total':<8}")
print("-"*90)

now_utc = datetime.now(timezone.utc)

for r in rows:
    latest = r['latest_utc']

    if latest is None:
        print(f"{r['station_id']:<12} {r['label']:<28} {'—':<20} \033[91mNEVER\033[0m{'':<14} {r['last_24h_count']:<6} {r['total_readings']:<8}")
        continue

    # Force UTC awareness
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    else:
        latest = latest.astimezone(timezone.utc)

    age_minutes = int((now_utc - latest).total_seconds() / 60)

    if age_minutes < 60:
        age_str = f"\033[92m{age_minutes}m ago\033[0m"
    elif age_minutes < 180:
        age_str = f"\033[93m{age_minutes}m ago\033[0m"
    else:
        age_str = f"\033[91m{age_minutes}m ago\033[0m"

    latest_str = latest.strftime("%Y-%m-%d %H:%M")
    print(f"{r['station_id']:<12} {r['label']:<28} {latest_str:<20} {age_str:<14} {r['last_24h_count']:<6} {r['total_readings']:<8}")

print("-"*90)
print("Green = fresh | Yellow = old | Red = dead")
print("="*90)