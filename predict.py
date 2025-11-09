import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA
from river_reference import STATIONS as stations

DB_FILE = '/home/river_levels_app/river_levels.db'

def get_historical_for_prediction(station_id):
    """Get historical levels for training (last 30 days, hourly resample)."""
    conn = sqlite3.connect(DB_FILE)
    start_date = (datetime.now() - timedelta(days=30)).isoformat()
    query = "SELECT timestamp, level FROM readings WHERE station_id = ? AND timestamp >= ? ORDER BY timestamp"
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    if df.empty or len(df) < 24:  # Need at least 1 day for ARIMA
        print(f"Not enough data for {station_id}")
        return None
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').resample('h').mean().ffill()  # Hourly, forward fill missing
    return df['level']

def train_and_predict(station_id):
    """Train ARIMA and predict 24h ahead from latest."""
    series = get_historical_for_prediction(station_id)
    if series is None:
        return None
    model = ARIMA(series, order=(5,1,0))  # Basic order; can tune
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=24)  # 24 hours ahead
    latest_ts = series.index[-1]
    predicted_for = [latest_ts + timedelta(hours=i+1) for i in range(24)]
    return list(zip(forecast, [ts.isoformat() for ts in predicted_for]))

def insert_prediction(station_id, predicted_level, predicted_for):
    """Insert or update prediction."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    created_at = datetime.now().isoformat()
    cursor.execute("INSERT OR REPLACE INTO predictions (station_id, predicted_level, predicted_for, created_at) VALUES (?, ?, ?, ?)",
                   (station_id, predicted_level, predicted_for, created_at))
    conn.commit()
    conn.close()

def review_performance(station_id):
    """Compare past predictions to actuals, compute error, retrain if high."""
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
        if mae > 0.1:  # Threshold; adjust
            print(f"High error—retraining {station_id}")
            # Retrain logic: Could refit with new order, but for now, just rerun predict
            return mae
    return 0.0

# Run for all stations
for station_id in stations:
    review_performance(station_id)  # Check and potentially flag retrain
    predictions = train_and_predict(station_id)
    if predictions:
        for level, ts in predictions:
            insert_prediction(station_id, level, ts)
    print(f"Predictions updated for {station_id}")