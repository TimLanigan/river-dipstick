#!/usr/bin/env python3
"""
River Dipstick — FINAL PERFECTION
Full-width chart by default
Tiny, beautiful, dynamic legend ONLY when needed
Works perfectly on mobile and desktop
G SPOT uses good_level column → clean lime dots
"""

import streamlit as st
import psycopg2
import pandas as pd
import time
import json
import altair as alt
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
import os
from pathlib import Path
# Load custom CSS
def load_css():
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@wintermute-db:5432/river_levels_db'
REAL_LABEL = "Measured Level"

def load_css(file_path):
    with open(file_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.set_page_config(
    layout="wide",
    page_title="River Dipstick",
    page_icon="🎣",
    initial_sidebar_state="collapsed"
)

load_css("style.css")

# === SIDEBAR WITH CLEAR LABEL ===
with st.sidebar:
    st.markdown(
        """
        <h2 class="site-title">River Dipstick</h2>
        """,
        unsafe_allow_html=True
    )
    
    show_sweet_spot = st.toggle("Find G Spot", value=False, help="Highlight good fishing levels at key stations")
    show_predictions = st.toggle("Predict Level", value=False, help="Use Mk1 eyeball to predict level")
    show_rain = st.toggle("Rain History", value=False, help="Show historic rainfall on the chart")
    show_map = st.toggle("View maps", value=False, help="Show measuring station")
    
    st.markdown("---")
    days_options = [7, 14, 30, 60, 180, 365]
    selected_days = st.select_slider(
        "Graph history (days)",
        options=days_options,
        value=7,
    )

# === STATIONS & RULES ===
from river_reference import load_stations
STATIONS = load_stations()

RULES_FILE = Path("/app/data/rules.json")
if not RULES_FILE.exists():
    RULES_FILE = Path("/home/river_levels_app/rules.json")
try:
    with open(RULES_FILE, "r") as f:
        RULES = json.load(f)
except FileNotFoundError:
    RULES = {}

# === DATABASE HELPERS ===
def get_latest_readings():
    conn = psycopg2.connect(CONNECTION_STRING)
    df = pd.read_sql_query("""
        SELECT DISTINCT ON (station_id)
        station_id, river, label, level, timestamp
        FROM readings
        ORDER BY station_id, timestamp DESC
    """, conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def get_historical_data(station_id, days=selected_days):
    conn = psycopg2.connect(CONNECTION_STRING)
    start = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    df = pd.read_sql_query("""
        SELECT timestamp, level, good_level FROM readings
        WHERE station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """, conn, params=(station_id, start))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.rename(columns={'timestamp': 'Date', 'level': 'Level (metres)'})
        df['Type'] = REAL_LABEL
    return df

def get_predictions(station_id, days=selected_days):
    conn = psycopg2.connect(CONNECTION_STRING)
    start = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    df = pd.read_sql_query("""
        SELECT predicted_for, predicted_level FROM predictions
        WHERE station_id = %s AND predicted_for >= %s
        ORDER BY predicted_for
    """, conn, params=(station_id, start))
    conn.close()
    if not df.empty:
        df['predicted_for'] = pd.to_datetime(df['predicted_for'])
        df = df.rename(columns={'predicted_for': 'Date', 'predicted_level': 'Level (metres)'})
        df['Type'] = 'Predicted'
    return df

def get_rainfall_data(station_id, days=selected_days):
    conn = psycopg2.connect(CONNECTION_STRING)
    start = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    df = pd.read_sql_query("""
        SELECT timestamp, rainfall_mm FROM rainfall_readings
        WHERE level_station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """, conn, params=(station_id, start))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.rename(columns={'timestamp': 'Date', 'rainfall_mm': 'Rainfall (mm)'})
        df['Type'] = 'Rainfall'
    return df

# === AUTO REFRESH ===
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()
if time.time() - st.session_state.last_refresh > 60:
    st.session_state.last_refresh = time.time()
    st.rerun()



# === MAIN DASHBOARD ===
df = get_latest_readings()
if df.empty:
    st.write("No data yet.")
else:
    tabs = st.tabs(["Eden", "Ribble", "Lune", "Hodder"])

    for tab, river in zip(tabs, ["Eden", "Ribble", "Lune", "Hodder"]):
        with tab:
            stations = STATIONS.get(river, [])
            stations = sorted(stations, key=lambda x: x.get('lat', 0)) if river == "Eden" else sorted(stations, key=lambda x: x.get('lat', 0), reverse=True)
            river_df = df[df['river'] == river].copy()

            if river_df.empty:
                st.write("No data.")
                continue
          

            # === FINAL TABLE (no colour coding) ===
            latest = river_df.loc[river_df.groupby('station_id')['timestamp'].idxmax()]
            latest = latest.set_index('station_id').reindex([s['id'] for s in stations]).dropna(subset=['river']).reset_index()

            display_df = pd.DataFrame({
                'Station': latest['label'],
                'Level': latest['level'].round(2).astype(str) + "m",
                'Latest Reading': latest['timestamp'].dt.strftime("%d-%m-%Y @ %H:%M"),
                'station_id': latest['station_id']
            })

            # Clean table
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # === CHARTS FOR EACH STATION ===
            for station in stations:
                st.write(f"### {station['label']}")
                hist = get_historical_data(station['id'])
                if hist.empty:
                    st.write("No data.")
                    continue

                chart_data = hist.copy()
                legend_items = [(REAL_LABEL, "#ad36eeff")]

                # === LEVEL PREDICTION ===
                # graph 2 days into future for eyeballing
                if show_predictions:
                    # Extend chart into future (no prediction line)
                    if not chart_data.empty:
                        last_date = chart_data['Date'].max()
                        future_dates = pd.date_range(
                            start=last_date,
                            periods=3,  # ~2 days ahead
                            freq='D'
                        )
                        future_df = pd.DataFrame({
                            'Date': future_dates,
                            'Level (metres)': [None] * len(future_dates),
                            'Type': ['Measured Level'] * len(future_dates)
                        })
                        chart_data = pd.concat([chart_data, future_df], ignore_index=True)

                # Rain
                rain_df = get_rainfall_data(station['id'])
                if show_rain and not rain_df.empty:
                    chart_data = pd.concat([chart_data, rain_df], ignore_index=True)
                    legend_items.append((" Rainfall", "lightblue"))

                # === MAIN LEVEL LINE ===
                level_line = alt.Chart(chart_data).mark_line(strokeWidth=4).encode(
                    x=alt.X('Date:T', title='Date',
                            axis=alt.Axis(format='%b %d', tickCount=14)),
                    y=alt.Y('Level (metres):Q', axis=alt.Axis(title='Level (m)', titleColor='white')),
                    color=alt.Color('Type:N',
                        scale=alt.Scale(domain=[x[0] for x in legend_items], range=[x[1] for x in legend_items]),
                        legend=None
                    ),
                    strokeDash=alt.condition(
                        alt.datum.Type == REAL_LABEL,
                        alt.value([0]),
                        alt.value([6,4])
                    )
                ).transform_filter(
                    alt.FieldOneOfPredicate(field='Type', oneOf=[x[0] for x in legend_items if 'Rainfall' not in x[0]])
                )

                # === GOOD FISHING BAND (New implementation - old lime dots removed) ===
                if show_sweet_spot:
                    station_id = station['id']
                    
                    if station_id in RULES and "good_min" in RULES[station_id] and "good_max" in RULES[station_id]:
                        good_min = RULES[station_id]["good_min"]
                        good_max = RULES[station_id]["good_max"]
                        
                        # Use full chart range (including future space)
                        if 'chart_data' in locals() and not chart_data.empty:
                            min_date = chart_data['Date'].min()
                            max_date = chart_data['Date'].max()
                        else:
                            min_date = hist['Date'].min()
                            max_date = hist['Date'].max()
                        
                        band_data = pd.DataFrame({
                            'Date': [min_date, max_date],
                            'ymin': [good_min, good_min],
                            'ymax': [good_max, good_max]
                        })
                        
                        good_band = alt.Chart(band_data).mark_rect(
                            color='#34d399',      # Vibrant modern green
                            opacity=0.50          # Lower opacity for subtlety
                        ).encode(
                            x=alt.X('Date:T'),
                            y=alt.Y('ymin:Q'),
                            y2=alt.Y2('ymax:Q')
                        )
                        
                        # Layer band under the main line
                        level_line = alt.layer(good_band, level_line)
                        
                        legend_items.append(("Good Fishing level", "#22c584"))

                    
                # === RAIN BARS ===
                rain_bars = alt.Chart(chart_data).mark_bar(opacity=0.1, size=5).encode(
                    x=alt.X('Date:T'),
                    y=alt.Y('Rainfall (mm):Q', axis=alt.Axis(title='Rain (mm)', titleColor='white')),
                    color=alt.value('lightblue')
                ).transform_filter(alt.datum.Type == 'Rainfall')

                # === FINAL CHART ===
                chart = level_line
                if show_rain and not rain_df.empty:
                    chart = alt.layer(level_line, rain_bars).resolve_scale(y='independent')

                # === LEGEND + CHART ===
                if len(legend_items) == 1:
                    st.altair_chart(chart, use_container_width=True)
                else:
                    legend_html = '<div style="text-align:right; margin:10px 0; padding:6px; border-radius:8px; font-size:0.8em;">'
                    for label, color in legend_items:
                        legend_html += f'<span style="margin:0 12px; display:inline-flex; align-items:center;">'
                        legend_html += f'<div style="width:16px; height:4px; background:{color}; border-radius:2px; margin-right:6px;"></div>{label}</span>'
                    legend_html += '</div>'
                    st.markdown(legend_html, unsafe_allow_html=True)
                    st.altair_chart(chart, use_container_width=True)

                if show_map and station.get('lat') and station.get('lon'):
                    # Higher zoom for better detail when "View maps" is toggled
                    st.map(
                        pd.DataFrame([{"lat": station['lat'], "lon": station['lon']}]),
                        zoom=13,          # Increased from 11 → closer view
                        use_container_width=True
                    )