import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA
from river_reference import STATIONS

DB_FILE = '/home/river_levels_app/river_levels.db'

def get_historical_for_prediction(station_id):
    conn = sqlite3.connect(DB_FILE)
    start_date = (datetime.now() - timedelta(days=30)).isoformat()
    query = "SELECT timestamp, level FROM readings WHERE station_id = ? AND timestamp >= ? ORDER BY timestamp"
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    if df.empty or len(df) < 24:
        print(f"Not enough data for {station_id}")
        return None
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').resample('h').mean().ffill()
    return df['level']

def train_and_predict(station_id):
    series = get_historical_for_prediction(station_id)
    if series is None:
        return None
    model = ARIMA(series, order=(5,1,0))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=24)
    latest_ts = series.index[-1]
    predicted_for = [latest_ts + timedelta(hours=i+1) for i in range(24)]
    return list(zip(forecast, [ts.isoformat() for ts in predicted_for]))

def insert_prediction(station_id, predicted_level, predicted_for):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    created_at = datetime.now().isoformat()
    cursor.execute("INSERT OR REPLACE INTO predictions (station_id, predicted_level, predicted_for, created_at) VALUES (?, ?, ?, ?)",
                   (station_id, predicted_level, predicted_for, created_at))
    conn.commit()
    conn.close()

def review_performance(station_id):
    conn = sqlite3.connect(DB_FILE)
    query = "SELECT predicted_level, predicted_for FROM predictions WHERE station_id = ? ORDER BY created_at DESC LIMIT 24"
    preds = pd.read_sql_query(query, conn, params=(station_id,))
    if preds.empty:
        return 0.0
    actuals = []
    for _, row in preds.iterrows():
        actual_query = "SELECT level FROM readings WHERE station_id = ? AND timestamp = ?"
        cursor = conn.cursor()
        cursor.execute(actual_query, (station_id, row['predicted_for']))
        actual = cursor.fetchone()
        if actual:
            actuals.append(actual[0])
    conn.close()
    if actuals:
        mae = sum(abs(p - a) for p, a in zip(preds['predicted_level'], actuals)) / len(actuals)
        print(f"MAE for {station_id}: {mae}")
        if mae > 0.1:
            print(f"High error—retraining {station_id}")
            return mae
    return 0.0

# Run for all stations
for river, station_list in STATIONS.items():
    for station in station_list:
        station_id = station['id']
        review_performance(station_id)
        predictions = train_and_predict(station_id)
        if predictions:
            for level, ts in predictions:
                insert_prediction(station_id, level, ts)
        print(f"Predictions updated for {station_id}")