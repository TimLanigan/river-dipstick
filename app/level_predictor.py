#!/usr/bin/env python3
"""
LIVE PREDICTOR — FINAL VERSION (no flat line)
Uses current state + iterative prediction
"""
import pandas as pd
from sqlalchemy import create_engine, text
import joblib
import os
from datetime import datetime, timedelta
from river_reference import STATIONS

DB_PASSWORD = os.getenv("DB_PASSWORD")
engine = create_engine(f"postgresql://river_user:{DB_PASSWORD}@db/river_levels_db")
MODEL_DIR = "/app/models"

def insert_prediction(station_id, level, predicted_for):
    ts_str = predicted_for.strftime('%Y-%m-%d %H:%M:%S')
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO predictions (station_id, predicted_level, predicted_for, created_at)
                VALUES (:sid, :level, :ts, NOW())
                ON CONFLICT (station_id, predicted_for) DO UPDATE
                SET predicted_level = EXCLUDED.predicted_level, created_at = NOW()
            """),
            [{"sid": station_id, "level": level, "ts": ts_str}]
        )
        conn.commit()

print("Starting live prediction run...")

for river, stations in STATIONS.items():
    for station in stations:
        sid = station['id']
        model_path = f"{MODEL_DIR}/{sid}_hgboost.pkl"
        if not os.path.exists(model_path):
            continue

        model = joblib.load(model_path)

        # Pull last 500 hours
        df = pd.read_sql(f"""
            SELECT r.timestamp::timestamptz AT TIME ZONE 'UTC' as ts, r.level,
                   COALESCE(rf.rainfall_mm, 0) as rain
            FROM readings r
            LEFT JOIN rainfall_readings rf ON rf.level_station_id = r.station_id
                                          AND rf.timestamp::timestamptz = r.timestamp::timestamptz
            WHERE r.station_id = %s
            ORDER BY r.timestamp DESC
            LIMIT 500
        """, engine, params=(sid,))

        if df.empty or len(df) < 50:
            continue

        df['ts'] = pd.to_datetime(df['ts']).dt.tz_localize(None)
        df = df.set_index('ts').sort_index().resample('h').mean().ffill()

        # Features
        data = df.copy()
        for lag in [1,3,6,12,24]:
            data[f'level_lag_{lag}'] = data['level'].shift(lag)
            data[f'rain_lag_{lag}'] = data['rain'].shift(lag)
        data['hour'] = data.index.hour
        data['dayofweek'] = data.index.dayofweek
        data['rolling_6h_mean'] = data['level'].rolling(6).mean()
        data = data.dropna()

        if len(data) < 24:
            continue

        X = data.drop(columns=['level'])

        # Current state (last row)
        current_features = X.iloc[-1:].copy()

        # Predict next 24 hours iteratively
        now = datetime.now()
        future_start = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        future_times = pd.date_range(start=future_start, periods=24, freq='h')

        preds = []
        features = current_features.copy()

        for i in range(24):
            pred = model.predict(features)[0]
            preds.append(pred)

            # Update features for next hour
            features = features.copy()
            features['level_lag_1'] = pred
            for lag in [3,6,12,24]:
                if i + 1 >= lag:
                    features[f'level_lag_{lag}'] = preds[i + 1 - lag]
            features['hour'] = future_times[i].hour
            features['dayofweek'] = future_times[i].dayofweek
            features['rolling_6h_mean'] = features['level_lag_1'].rolling(6).mean().iloc[-1]

        # Insert
        for ts, pred in zip(future_times, preds):
            insert_prediction(sid, round(float(pred), 6), ts)

        print(f"Updated 24h future for {sid} — {station['label']}")

print("Live prediction run complete — refresh site!")