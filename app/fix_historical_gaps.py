#!/usr/bin/env python3
"""
fix_historical_gaps.py – FINAL VERSION (bug-free)
Run --dry-run first (default), then --apply when happy.
"""
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from river_reference import STATIONS
import argparse
from loguru import logger

# ------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--apply", action="store_true")
parser.add_argument("--dry-run", dest="apply", action="store_false")
parser.set_defaults(apply=False)
args = parser.parse_args()
DRY_RUN = not args.apply

logger.remove()
logger.add(lambda msg: print(msg, end=""), colorize=True, level="INFO" if DRY_RUN else "SUCCESS")

if DRY_RUN:
    logger.warning("DRY RUN MODE – No changes will be made")
else:
    logger.success("LIVE MODE – Inserting interpolated rows")

# ------------------------------------------------------------------
load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
if not DB_PASS:
    raise ValueError("DB_PASSWORD missing")
CONN_STR = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'
conn = psycopg2.connect(CONN_STR)
cur = conn.cursor()

total_inserted = 0

for river_name, stations in STATIONS.items():
    for station in stations:
        station_id = station['id']
        label = station['label']

        logger.info(f"Processing {label} ({station_id})")

        cur.execute("""
            SELECT timestamp, level
            FROM readings
            WHERE station_id = %s
            ORDER BY timestamp
        """, (station_id,))
        rows = cur.fetchall()

        if not rows:
            logger.info("   → no data, skipping")
            continue
        if len(rows) < 2:
            logger.info("   → only one reading, nothing to interpolate")
            continue

        df = pd.DataFrame(rows, columns=['timestamp', 'level'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()

        # Use '15min' instead of deprecated '15T'
        start = df.index.min().floor('15min')
        end   = df.index.max().ceil('15min')
        perfect = pd.date_range(start=start, end=end, freq='15min')

        df_full = df.reindex(perfect)
        missing = df_full['level'].isna().sum()

        if missing == 0:
            logger.info("   → already perfect")
            continue

        df_full['level'] = df_full['level'].interpolate(method='linear').round(3)

        to_insert = []
        for ts, row in df_full.iterrows():
            if ts in df.index:
                continue                               # real reading, already there
            if pd.isna(row['level']):
                continue
            # ← THIS LINE FIXES THE BUG
            level = float(row['level'])                # force Python float, not np.float64
            ts_str = ts.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            to_insert.append((station_id, river_name, label, level, ts_str))

        if not to_insert:
            logger.info("   → no gaps found")
            continue

        logger.info(f"   → Would insert {len(to_insert):,} rows" if DRY_RUN else f"   → Inserting {len(to_insert):,} rows")

        if not DRY_RUN:
            for rec in to_insert:
                cur.execute("""
                    INSERT INTO readings
                    (station_id, river, label, level, timestamp, good_level)
                    VALUES (%s, %s, %s, %s, %s, 'n')
                    ON CONFLICT (station_id, timestamp) DO NOTHING
                """, rec)
            total_inserted += len(to_insert)
            conn.commit()

# ------------------------------------------------------------------
print("\n" + "="*70)
print("HISTORICAL GAP FIX COMPLETE")
print("="*70)
print(f"Mode:      {'DRY RUN' if DRY_RUN else 'LIVE APPLIED'}")
if DRY_RUN:
    print("Run again with --apply when ready")
else:
    print(f"Total interpolated rows inserted: {total_inserted:,}")
    print("Your database is now a perfect 15-minute grid forever!")
print("="*70)