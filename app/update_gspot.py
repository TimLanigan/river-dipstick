#!/usr/bin/env python3
"""
update_gspot.py – Incremental update for good_level flags.
Run every 15 mins after data pull. Processes only recent un-flagged readings.
Place in the same directory as backfill_gspot.py (or /app/utility/ in collector).
"""
import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONN = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'  # Adjust if your conn string differs

# Path fallback like backfill
RULES_PATH = Path("/app/data/rules.json")
if not RULES_PATH.exists():
    RULES_PATH = Path("/home/river_levels_app/rules.json")  # Or your host path
RULES = json.loads(RULES_PATH.read_text())

UTC = ZoneInfo("UTC")

# Log to file for easy checking
logger.add("/opt/river-dipstick/gspot_update.log", rotation="10 MB", level="INFO")

def update_station(station_id):
    conn = psycopg2.connect(CONN)
    cur = conn.cursor()

    # Look back 2 days – covers rain window with buffer, keeps it fast
    since_dt = datetime.now(UTC) - timedelta(days=2)
    cur.execute("""
        SELECT timestamp, level
        FROM readings
        WHERE station_id = %s AND timestamp >= %s AND good_level = 'n'
        ORDER BY timestamp
    """, (station_id, since_dt))
    rows = cur.fetchall()

    if not rows:
        logger.info(f"No pending readings for {station_id}")
        conn.close()
        return

    logger.info(f"Updating G SPOT for {station_id} — {len(rows)} readings")
    cfg = RULES.get(station_id, {}).get("good_fishing")
    if not cfg:
        logger.warning(f"No rules for {station_id}")
        conn.close()
        return

    start_lvl = cfg["falling_start"]
    end_lvl = cfg["falling_end"]
    rain_thr = cfg["rain_threshold"]
    gspot_count = 0

    for ts_tz, level in rows:
        two_h_ago = ts_tz - timedelta(hours=2)
        fourteen_d_ago = ts_tz - timedelta(days=14)

        # Falling check (last ~2 hours, need 4+ points, non-increasing)
        cur.execute("""
            SELECT level FROM readings
            WHERE station_id = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp
        """, (station_id, two_h_ago, ts_tz))
        recent = [r[0] for r in cur.fetchall()]
        falling = len(recent) >= 4 and all(recent[i] >= recent[i+1] for i in range(len(recent)-1))

        # In band
        in_band = end_lvl <= level <= start_lvl

        # 14-day rain total >= threshold (as per original code)
        cur.execute("""
            SELECT COALESCE(SUM(rainfall_mm), 0)
            FROM rainfall_readings
            WHERE level_station_id = %s
              AND timestamp::timestamptz >= %s
        """, (station_id, fourteen_d_ago))
        rain_total = cur.fetchone()[0] or 0
        rain_ok = rain_total >= rain_thr

        flag = 'y' if (falling and in_band and rain_ok) else 'n'
        if flag == 'y':
            gspot_count += 1

        cur.execute("""
            UPDATE readings SET good_level = %s
            WHERE station_id = %s AND timestamp = %s
        """, (flag, station_id, ts_tz))

    conn.commit()
    conn.close()
    logger.success(f"Finished {station_id} → {gspot_count} new G SPOT hits")

if __name__ == "__main__":
    for sid in RULES.keys():
        update_station(sid)
    print("Incremental G SPOT update complete!")