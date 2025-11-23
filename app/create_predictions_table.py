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
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    station_id TEXT NOT NULL,
    predicted_for TIMESTAMP WITH TIME ZONE NOT NULL,
    predicted_level DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(station_id, predicted_for)
);
CREATE INDEX IF NOT EXISTS idx_predictions_station ON predictions(station_id);
CREATE INDEX IF NOT EXISTS idx_predictions_time ON predictions(predicted_for);
""")

conn.commit()
cur.close()
conn.close()
print("predictions table ready")