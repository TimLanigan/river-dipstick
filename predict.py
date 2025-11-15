#!/usr/bin/env python3
"""
predict.py - River Dipstick ML Predictions
Now with: RAINFALL REGRESSOR via Prophet
Runs every hour via cron
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta, UTC
from sqlalchemy import create_engine
from prophet import Prophet
from river_reference import STATIONS
import time
import warnings
warnings.filterwarnings("ignore")

CONNECTION_STRING = 'postgresql://river_user:***REMOVED***@localhost/river_levels_db'

def get_engine():
    return create_engine(CONNECTION_STRING)

def get_historical_for_prediction(station_id, rainfall_id):
    engine = get_engine()
    
    # === GET LEVELS (30 days, hourly) ===
    start_date = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    level_query = """
        SELECT timestamp, level 
        FROM readings 
        WHERE station_id = %s AND timestamp >= %s 
        ORDER BY timestamp
    """
    level_df = pd.read_sql_query(level_query, engine, params=(station_id, start_date))
    
    if level_df.empty or len(level_df) < 48:
        print(f"Not enough level data for {station_id}")
        return None

    # === GET RAINFALL (7 days, hourly) ===
    rain_query = """
        SELECT timestamp, rainfall_mm 
        FROM rainfall_readings 
        WHERE level_station_id = %s AND timestamp >= %s 
        ORDER BY timestamp
    """
    rain_start = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    rain_df = pd.read_sql_query(rain_query, engine, params=(station_id, rain_start))
    
    # === RESAMPLE TO HOURLY + STRIP TIMEZONE ===
    level_df['timestamp'] = pd.to_datetime(level_df['timestamp'], utc=True).dt.tz_convert(None)
    level_df = level_df.set_index('timestamp').resample('h').mean().ffill().reset_index()
    level_df = level_df.rename(columns={'timestamp': 'ds', 'level': 'y'})

    # Merge rain
    if not rain_df.empty:
        rain_df['timestamp'] = pd.to_datetime(rain_df['timestamp'], utc=True).dt.tz_convert(None)
        rain_df = rain_df.set_index('timestamp').resample('h').sum().reset_index()
        rain_df = rain_df.rename(columns={'timestamp': 'ds', 'rainfall_mm': 'rainfall'})
        df = level_df.merge(rain_df[['ds', 'rainfall']], on='ds', how='left')
        df['rainfall'] = df['rainfall'].fillna(0)
    else:
        level_df['rainfall'] = 0
        df = level_df

    return df

def train_and_predict(station_id, rainfall_id):
    df = get_historical_for_prediction(station_id, rainfall_id)
    if df is None:
        return None

    # === PROPHET WITH RAIN REGRESSOR ===
    m = Prophet(
        yearly_seasonality=False,
        weekly_seasonality=True,
        daily_seasonality=True,
        changepoint_prior_scale=0.05
    )
    m.add_regressor('rainfall')
    m.fit(df)

    # === FORECAST 24H ===
    future = m.make_future_dataframe(periods=24, freq='h')
    future['ds'] = future['ds'].dt.tz_localize(None)  # ← STRIP TZ
    
    # Add future rainfall (assume 0 for now — can plug in forecast later)
    future = future.merge(df[['ds', 'rainfall']], on='ds', how='left')
    future['rainfall'] = future['rainfall'].fillna(0)

    forecast = m.predict(future)
    forecast = forecast[['ds', 'yhat']].tail(24)
    forecast['yhat'] = forecast['yhat'].clip(lower=0)  # No negative levels

    return list(zip(forecast['yhat'], forecast['ds'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')))

def insert_prediction(station_id, predicted_level, predicted_for):
    conn = psycopg2.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    created_at = datetime.now(UTC).isoformat()
    try:
        cursor.execute("""
            INSERT INTO predictions (station_id, predicted_level, predicted_for, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (station_id, predicted_for) DO UPDATE
            SET predicted_level = EXCLUDED.predicted_level, created_at = EXCLUDED.created_at
        """, (station_id, predicted_level, predicted_for, created_at))
        conn.commit()
    except Exception as e:
        print(f"Error inserting prediction for {station_id}: {e}")
    finally:
        conn.close()

# === MAIN ===
if __name__ == "__main__":
    print(f"Starting Prophet + Rain predictions: {datetime.now(UTC)}")
    for river, station_list in STATIONS.items():
        for station in station_list:
            station_id = station['id']
            rainfall_id = station.get('rainfall_id')
            if not rainfall_id:
                print(f"No rainfall_id for {station_id} — skipping rain regressor")
                continue

            print(f"Predicting {station['label']} ({station_id}) with rain from {rainfall_id}...")
            predictions = train_and_predict(station_id, rainfall_id)
            if predictions:
                for level, ts in predictions:
                    insert_prediction(station_id, level, ts)
                print(f"  → 24h forecast updated")
            else:
                print(f"  → No prediction generated")
            time.sleep(1)  # Be kind to DB
    print("All predictions complete.")