#!/usr/bin/env python3
"""
FINAL backfill_gspot.py – works on your exact DB
rainfall_readings.timestamp = TEXT → we cast it to timestamptz
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
CONN = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

RULES_PATH = Path("/app/data/rules.json")
if not RULES_PATH.exists():
    RULES_PATH = Path("/home/river_levels_app/rules.json")
RULES = json.loads(RULES_PATH.read_text())

UTC = ZoneInfo("UTC")

def recompute_station(station_id):
    conn = psycopg2.connect(CONN)
    cur = conn.cursor()

    since_dt = datetime.now(UTC) - timedelta(days=30)

    cur.execute("""
        SELECT timestamp, level
        FROM readings
        WHERE station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """, (station_id, since_dt))

    rows = cur.fetchall()
    logger.info(f"Recomputing G SPOT for {station_id} — {len(rows)} readings")

    cfg = RULES.get(station_id, {}).get("good_fishing")
    if not cfg:
        logger.warning(f"No rules for {station_id}")
        conn.close()
        return

    start_lvl = cfg["falling_start"]
    end_lvl   = cfg["falling_end"]
    rain_thr  = cfg["rain_threshold"]

    gspot_count = 0

    for ts_tz, level in rows:
        two_h_ago      = ts_tz - timedelta(hours=2)
        fourteen_d_ago = ts_tz - timedelta(days=14)

        # 1. Falling?
        cur.execute("""
            SELECT level FROM readings
            WHERE station_id = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp
        """, (station_id, two_h_ago, ts_tz))
        recent = [r[0] for r in cur.fetchall()]
        falling = len(recent) >= 4 and all(recent[i] >= recent[i+1] for i in range(len(recent)-1))

        # 2. In band?
        in_band = end_lvl <= level <= start_lvl

        # 3. 14-day rainfall — TEXT column → explicit cast
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
    logger.success(f"Finished {station_id} → {gspot_count} G SPOT hits")

if __name__ == "__main__":
    for sid in ["713040", "710301", "760112"]:
        if sid in RULES:
            recompute_station(sid)
    print("\nBackfill complete! Green dots await you.")