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

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@wintermute-db:5432/river_levels_db'
REAL_LABEL = "Measured Level"

st.set_page_config(layout="wide", page_title="River Dipstick", page_icon="🎣")

st.markdown("""<style> 
.main .block-container {padding-top: 0rem; margin-top: 0px;}#MainMenu { margin-top: -1rem; }
</style>
""", unsafe_allow_html=True)

# === STATIONS & RULES (only good_fishing part is used for G-spot) ===
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

def get_historical_data(station_id, days=14):
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

def get_predictions(station_id, days=14):
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

def get_rainfall_data(station_id, days=14):
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

# === HEADER ===
st.markdown("""
<div style="text-align:center; padding:0.5rem 0 1rem; margin-top: -5.5rem;">
  <h1 style="
    font-size:2rem;
    font-weight:800;
    background:linear-gradient(90deg,#ff1493,#ff69b4,#ff1493);
    -webkit-background-clip:text;
    background-clip:text;
    color:transparent;
    text-shadow:0 0 30px rgba(255,20,147,0.4);
    margin:0;
    letter-spacing:-1px;
  ">River Dipstick</h1>
</div>
""", unsafe_allow_html=True)

# === TOGGLES ===
c1, c2, c3, c4 = st.columns(4)
with c1: show_predictions = st.toggle("Level Predictions", False)
with c2: show_sweet_spot = st.toggle("Find the G Spot", False)
with c3: show_rain = st.toggle("Rain History", False)
with c4: show_map = st.toggle("Map", False)

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

            # === FINAL TABLE (no colour coding any more) ===
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
                legend_items = [(REAL_LABEL, "steelblue")]

                # Predictions
                if show_predictions:
                    pred = get_predictions(station['id'])
                    if not pred.empty:
                        now = datetime.now(UTC)
                        past = pred[pred['Date'] < now].copy()
                        past['Type'] = 'Past Performance'
                        future = pred[pred['Date'] >= now].copy()
                        future['Type'] = 'Future Prediction'
                        chart_data = pd.concat([chart_data, past, future], ignore_index=True)
                        legend_items += [("Past Performance", "#888888"), ("Future Prediction", "#BB22BB")]

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

                # === G SPOT (completely untouched) ===
                if show_sweet_spot:
                    gspot_rows = hist[hist.get('good_level') == 'y'].copy()
                    if not gspot_rows.empty:
                        gspot_dots = alt.Chart(gspot_rows).mark_circle(
                            size=20,
                            color='lime',
                            opacity=1,
                            stroke='lime',
                            strokeWidth=2
                        ).encode(
                            x='Date:T',
                            y=alt.Y('Level (metres):Q', title='Level (m)'),
                            tooltip=['Date:T', 'Level (metres):Q']
                        )
                        level_line = level_line + gspot_dots
                        legend_items.append(("Good Level", "lime"))

                    if not hist.empty and hist.iloc[-1].get('good_level') == 'y':
                        st.markdown("""
                        <h4 style="color:limegreen; text-align:right; font-size:0.8rem;">
                        G Spot found, go fishing!!!
                        </h4>
                        """, unsafe_allow_html=True)

                # === RAIN BARS ===
                rain_bars = alt.Chart(chart_data).mark_bar(opacity=0.2, size=5).encode(
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
                    st.map(pd.DataFrame([{"lat": station['lat'], "lon": station['lon']}]), zoom=11)

# === FOOTER ===
st.markdown("""
<div style="text-align: center; margin-top: 50px; color: #888; font-size: 0.9rem;">
All raw data sourced from the Environment Agency API<br>
Built with Streamlit & Prophet ML<br>
Vibe-coded by tim<br>
<a href="https://buymeacoffee.com/riverdipstick" target="_blank">Buy me a coffee if I helped you catch a fish</a>
</div>
""", unsafe_allow_html=True)