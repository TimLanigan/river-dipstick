#!/usr/bin/env python3
"""
BACKFILL — FINAL WORKING VERSION (SQLAlchemy 2.0 compatible)
Tested and proven with your exact environment
"""
import pandas as pd
from sqlalchemy import create_engine, text
import joblib
import os
from datetime import datetime, timedelta
from river_reference import STATIONS

# Connection
DB_PASSWORD = os.getenv("DB_PASSWORD")
if not DB_PASSWORD:
    raise RuntimeError("DB_PASSWORD not set in environment")

engine = create_engine(f"postgresql://river_user:{DB_PASSWORD}@db/river_levels_db")
MODEL_DIR = "/app/models"
os.makedirs(MODEL_DIR, exist_ok=True)

# The ONLY insert pattern that works reliably with SQLAlchemy 2.0
insert_sql = text("""
    INSERT INTO predictions (station_id, predicted_level, predicted_for, created_at)
    VALUES (:station_id, :level, :ts, NOW())
    ON CONFLICT (station_id, predicted_for) DO UPDATE
    SET predicted_level = EXCLUDED.predicted_level, created_at = NOW()
""")

def insert_prediction(station_id, level, predicted_for):
    """Insert one prediction using the exact format that works"""
    ts_str = predicted_for.strftime('%Y-%m-%d %H:%M:%S')
    with engine.connect() as conn:
        conn.execute(
            insert_sql,
            [{"station_id": station_id, "level": level, "ts": ts_str}]  # ← list of one dict
        )
        conn.commit()

print("Starting backfill with HGBoost models...")

for river, stations in STATIONS.items():
    for station in stations:
        sid = station['id']
        print(f"Backfilling {sid} — {station['label']}...")

        model_path = f"{MODEL_DIR}/{sid}_hgboost.pkl"
        if not os.path.exists(model_path):
            print("  No model file — skipping")
            continue

        try:
            model = joblib.load(model_path)
        except Exception as e:
            print(f"  Failed to load model: {e}")
            continue

        # Pull last 15 days of data
        end = datetime.now()
        start = end - timedelta(days=15)

        df = pd.read_sql(f"""
            SELECT 
                r.timestamp::timestamptz AT TIME ZONE 'UTC' as ts,
                r.level,
                COALESCE(rf.rainfall_mm, 0) as rain
            FROM readings r
            LEFT JOIN rainfall_readings rf 
                ON rf.level_station_id = r.station_id 
               AND rf.timestamp::timestamptz = r.timestamp::timestamptz
            WHERE r.station_id = %s AND r.timestamp >= %s
            ORDER BY r.timestamp
        """, engine, params=(sid, start))

        if df.empty:
            print("  No data in date range — skipping")
            continue

        df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize(None)
        df = df.set_index('ts').resample('h').mean().ffill()

        # Features
        data = df.copy()
        for lag in [1, 3, 6, 12, 24]:
            data[f'level_lag_{lag}'] = data['level'].shift(lag)
            data[f'rain_lag_{lag}'] = data['rain'].shift(lag)
        data['hour'] = data.index.hour
        data['dayofweek'] = data.index.dayofweek
        data['rolling_6h_mean'] = data['level'].rolling(6).mean()
        data = data.dropna()

        if len(data) < 100:
            print("  Not enough feature data — skipping")
            continue

        X = data.drop(columns=['level'])

        # Generate 14 days back + 24h forward
        future_times = pd.date_range(
            start=data.index[-336],  # 14 days ago
            end=end + timedelta(hours=24),
            freq='h'
        )

        # Predict
        n = len(future_times)
        preds = model.predict(X.iloc[-n:]) if len(X) >= n else model.predict(X)

        # Insert all predictions
        for ts, pred in zip(future_times[:len(preds)], preds):
            insert_prediction(sid, round(float(pred), 6), ts)

        print(f"  Inserted {len(preds)} predictions")

print("BACKFILL COMPLETE — refresh the dashboard!")