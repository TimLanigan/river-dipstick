# analyse_inconsistencies.py
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np

# ------------------------------------------------------------------
# 1. Load the password from .env and connect to your real DB
# ------------------------------------------------------------------
load_dotenv()

DB_PASS = os.getenv("DB_PASSWORD")
if not DB_PASS:
    raise ValueError("DB_PASSWORD not found in .env file!")

CONN = f'postgresql://river_user:{DB_PASS}@db/river_levels_db'

engine = create_engine(CONN)

# ------------------------------------------------------------------
# 2. Pull ALL readings for the station we care about (760112)
#    Change the station_id if you want to analyse another one later
# ------------------------------------------------------------------
station_id = "760112"   # Great Musgrave Bridge - Eden

query = f"""
SELECT id, station_id, river, label, level, timestamp, good_level
FROM readings
WHERE station_id = '{station_id}'
ORDER BY timestamp DESC;
"""

print(f"Loading all data for station {station_id} ...")
df = pd.read_sql(query, engine)

print(f"Loaded {len(df):,} rows")

# ------------------------------------------------------------------
# 3. Make sure timestamp is proper datetime and sorted oldest → newest
# ------------------------------------------------------------------
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp').reset_index(drop=True)

# ------------------------------------------------------------------
# 4. Calculate time gaps in minutes
# ------------------------------------------------------------------
df['time_diff_min'] = df['timestamp'].diff().dt.total_seconds() / 60
# First row will be NaN - fill with 0 so stats work nicely
df['time_diff_min'] = df['time_diff_min'].fillna(0)

# ------------------------------------------------------------------
# 5. Count decimal places in the level column
# ------------------------------------------------------------------
def count_decimals(x):
    if pd.isna(x):
        return 0
    str_x = str(x).strip()
    if '.' in str_x:
        return len(str_x.split('.')[1])
    return 0

df['decimal_places'] = df['level'].apply(count_decimals)

# ------------------------------------------------------------------
# 6. Print a nice summary (copy-paste this output back to me!)
# ------------------------------------------------------------------
print("\n" + "="*60)
print("FULL DATABASE ANALYSIS - Station 760112 (Eden - Great Musgrave Bridge)")
print("="*60)

print(f"Total readings:           {len(df):,}")
print(f"Date range:               {df['timestamp'].min()}  →  {df['timestamp'].max()}")
print(f"Days covered:             {((df['timestamp'].max() - df['timestamp'].min()).days + 1)}")

print("\n--- Timestamp gaps (minutes) ---")
print(df['time_diff_min'].describe())
print("\nMost common gaps:")
print(df['time_diff_min'].value_counts().head(10))

print("\nGaps bigger than 30 minutes (these are the real problem ones):")
big_gaps = df[df['time_diff_min'] > 30][['timestamp', 'time_diff_min']]
print(big_gaps)

print("\n--- Level precision (decimal places) ---")
print(df['decimal_places'].value_counts().sort_index())

print("\nExample rows with different precision:")
print(df[['timestamp', 'level']].head(10))

print("\n" + "="*60)
print("ANALYSIS COMPLETE - copy everything above and send it back!")
print("="*60)