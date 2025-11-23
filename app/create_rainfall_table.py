#!/usr/bin/env python3
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
conn_str = f"host=db user=river_user password={DB_PASS} dbname=river_levels_db"

conn = psycopg2.connect(conn_str)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS rainfall_readings (
    id SERIAL PRIMARY KEY,
    level_station_id TEXT NOT NULL,
    rain_station_id TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    rainfall_mm DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(level_station_id, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_rainfall_station ON rainfall_readings(level_station_id);
CREATE INDEX IF NOT EXISTS idx_rainfall_time ON rainfall_readings(timestamp);
""")

conn.commit()
cur.close()
conn.close()
print("rainfall_readings table ready")