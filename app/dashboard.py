#!/usr/bin/env python3
"""
River Dipstick — FINAL PERFECTION
Full-width chart by default
Tiny, beautiful, dynamic legend ONLY when needed
Works perfectly on mobile and desktop
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
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db:5432/river_levels_db'
REAL_LABEL = "Measured Level"

st.set_page_config(layout="wide", page_title="River Dipstick")

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

# === STYLING FUNCTION ===
def apply_styles(row):
    styles = [''] * len(row)
    level_idx = row.index.get_loc('level')
    station_id = str(row['station_id'])
    level = row['level']
    if station_id in RULES and isinstance(RULES[station_id], dict) and 'levels' in RULES[station_id]:
        for rule in RULES[station_id]['levels']:
            min_val = rule['min']
            max_val = rule['max'] if rule['max'] is not None else float('inf')
            if min_val <= level < max_val:
                color = rule['color']
                if color == 'red':
                    styles[level_idx] = 'background-color: red; color: white;'
                elif color == 'yellow':
                    styles[level_idx] = 'background-color: yellow; color: black;'
                elif color == 'lightgreen':
                    styles[level_idx] = 'background-color: lightgreen; color: black;'
                break
    return styles

# === DATABASE HELPERS ===
def get_latest_readings():
    conn = psycopg2.connect(CONNECTION_STRING)
    df = pd.read_sql_query("""
        SELECT station_id, river, label, level, timestamp
        FROM readings
        WHERE id IN (SELECT MAX(id) FROM readings GROUP BY station_id)
    """, conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def get_historical_data(station_id, days=14):
    conn = psycopg2.connect(CONNECTION_STRING)
    start = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    df = pd.read_sql_query("""
        SELECT timestamp, level FROM readings
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
<div style="text-align: center; padding: 0px; margin-bottom: 10px">
    <div style="font-size: 2.2rem; font-weight: bold; color: grey;">River Dipstick</div>
    <div style="font-size: 1.2rem; color: violet; font-style: italic; margin-top: -10px;">a flyfisher's wet dream</div>
</div>
""", unsafe_allow_html=True)

# === DEV BADGE ===
if os.getenv("ENVIRONMENT", "production") != "production":
    st.markdown("""
        <div style="text-align: center; padding: 0px; margin-bottom: 10px">
        <div style="font-size: 0.8rem; color: grey; margin-top: -10px;">development site</div>
        </div>
    """, unsafe_allow_html=True)

# === TOGGLES ===
c1, c2, c3, c4 = st.columns(4)
with c1: show_predictions = st.toggle("Level Predictions", False)
with c2: show_sweet_spot = st.toggle("G Spot", False)
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

            # === FINAL TABLE ===
            latest = river_df.loc[river_df.groupby('station_id')['timestamp'].idxmax()]
            latest = latest.set_index('station_id').reindex([s['id'] for s in stations]).dropna(subset=['river']).reset_index()

            display_df = pd.DataFrame({
                'Station': latest['label'],
                'Level': latest['level'].round(2).astype(str) + "m",
                'Latest Reading': latest['timestamp'].dt.strftime("%d-%m-%Y @ %H:%M"),
                'station_id': latest['station_id']
            })

            def color_level_column(row):
                station_id = str(row['station_id'])
                level = latest.iloc[row.name]['level']
                bg = ''
                if station_id in RULES and 'levels' in RULES[station_id]:
                    for rule in RULES[station_id]['levels']:
                        min_val = rule['min']
                        max_val = rule['max'] if rule['max'] is not None else float('inf')
                        if min_val <= level < max_val:
                            color = rule['color']
                            if color == 'red':
                                bg = 'background-color: red; color: white;'
                            elif color == 'yellow':
                                bg = 'background-color: yellow; color: black;'
                            elif color == 'lightgreen':
                                bg = 'background-color: lightgreen; color: black;'
                            break
                return ['', bg, '', '']
            styled = display_df.style.apply(color_level_column, axis=1)
            st.dataframe(styled, use_container_width=True, hide_index=True)

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


                # === G SPOT — segmented, no straight-line joins, plays nice with others ===
                level_line = alt.Chart(chart_data).mark_line(strokeWidth=4).encode(
                    x=alt.X('Date:T', title='Date'),
                    y=alt.Y('Level (metres):Q', axis=alt.Axis(title='Level (m)', titleColor='blue')),
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

                if show_sweet_spot:
                    cfg = RULES.get(str(station['id']), {}).get('good_fishing', {})
                    if cfg:
                        falling_start = cfg.get('falling_start', 0.9)
                        falling_end = cfg.get('falling_end', 0.4)
                        rain_threshold = cfg.get('rain_threshold', 8)

                        total_rain = rain_df['Rainfall (mm)'].sum() if not rain_df.empty else 0
                        rain_ok = total_rain >= rain_threshold

                        # Mark G-Spot points
                        hist['in_gspot'] = False
                        for i in range(len(hist)):
                            cur_level = hist.iloc[i]['Level (metres)']
                            start_idx = max(0, i - 7)
                            recent = hist['Level (metres)'].iloc[start_idx:i+1]
                            falling = len(recent) >= 4 and recent.is_monotonic_decreasing
                            in_range = falling_end <= cur_level <= falling_start
                            if falling and in_range and rain_ok:
                                hist.loc[hist.index[i], 'in_gspot'] = True

                        gspot_data = hist[hist['in_gspot']].copy()

                        if not gspot_data.empty:
                            # Create segment column using index position (not datetime)
                            gspot_data = gspot_data.copy()
                            # Reset index so we can use row numbers
                            gspot_data = gspot_data.reset_index(drop=True)
                            # New segment every time there's a gap of 1 or more rows
                            gspot_data['segment'] = (gspot_data.index.to_series().diff() > 1).cumsum()
                            gspot_line = alt.Chart(gspot_data).mark_line(strokeWidth=5, color='#00ff00').encode(
                                x='Date:T',
                                y='Level (metres):Q',
                                detail='segment:N'  # Force nominal type — no more errors
                            )
                            level_line = alt.layer(level_line, gspot_line)
                            legend_items.append(("G SPOT!", "#00ff00"))
                        
                        if hist.iloc[-1]['in_gspot']:
                            st.markdown("<h3 style='color:lime; text-align:center; margin:20px 0;'>G SPOT ACTIVE!!</h3>", unsafe_allow_html=True)

                # === RAIN BARS ===
                rain_bars = alt.Chart(chart_data).mark_bar(opacity=0.3, size=3).encode(
                    x=alt.X('Date:T'),
                    y=alt.Y('Rainfall (mm):Q', axis=alt.Axis(title='Rain (mm)', titleColor='lightblue')),
                    color=alt.value('lightblue')
                ).transform_filter(alt.datum.Type == 'Rainfall')

                # === FINAL CHART ===
                chart = level_line
                if show_rain and not rain_df.empty:
                    chart = (level_line + rain_bars).resolve_scale(y='independent')
                chart = chart.properties(height=320)

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