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
import pytz
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

# UK Timezone helper (handles BST <-> GMT automatically)
def to_uk_time(utc_dt):
    uk_tz = pytz.timezone('Europe/London')
    if isinstance(utc_dt, str):
        utc_dt = pd.to_datetime(utc_dt)
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.tz_localize('UTC')
    return utc_dt.tz_convert(uk_tz)

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

def get_historical_data(station_id, days=7):
    conn = psycopg2.connect(CONNECTION_STRING)
    start = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    df = pd.read_sql_query("""
        SELECT timestamp, level 
        FROM readings
        WHERE station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """, conn, params=(station_id, start))
    conn.close()
    
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['timestamp'] = df['timestamp'].apply(to_uk_time)
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
        df['predicted_for'] = df['predicted_for'].apply(to_uk_time)
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
    # Get all rivers dynamically and sort alphabetically
    all_rivers = sorted(STATIONS.keys())
    tab_list = all_rivers + ["About"]
    
    tabs = st.tabs(tab_list)
    
    for tab, river in zip(tabs, tab_list):
        with tab:
            if river == "About":
                # === ABOUT PAGE ===
                st.write("The **River Dipstick** is built by a Lancashire fly fisherman primarly for himself and other local legends.")
                st.write("All data is sourced from the EA and SEPA public API's.")
                st.markdown("### Features")
                st.write("- Access the sidebar by selecting the menu >> in the top lefthand corner of the site\n- Select 'Find G Spot' to highlight good fishing levels on selected charts, based on local wisdom (where it exists)\n- When viewing the 'Good Fishing Band' on a chart; remember... a falling river is always best\n- Select 'Predict Level' for space to eyeball trends\n- Select 'Rainfall History' to view recent rainfall data\n- Select 'Maps' to see where the measuring station is located\n- Use the 'Graph History' slider to show more or less data on the charts\n- **Don't spend too long looking at data, if in doubt... go fishing** 😊")
                st.markdown("### Tech")
                st.write("100% open source - [https://github.com/TimLanigan/river-dipstick]")
                st.write("Built with Streamlit • PostgreSQL • Altair • Docker")
                st.markdown("### Feedback")
                st.write("Coming Soon")
                continue

            # === NORMAL RIVER TAB ===
            stations = STATIONS.get(river, [])
            # Sort stations by latitude (north to south or vice versa)
            stations = sorted(stations, key=lambda x: x.get('lat', 0), reverse=True)
            
            river_df = df[df['river'] == river].copy()
            if river_df.empty:
                st.write("No data.")
                continue

            # Table + Charts code (your existing code)
            latest = river_df.loc[river_df.groupby('station_id')['timestamp'].idxmax()]
            latest = latest.set_index('station_id').reindex([s['id'] for s in stations]).dropna(subset=['river']).reset_index()
            # Convert to UK time for display
            latest_display = latest.copy()
            latest_display['timestamp'] = latest_display['timestamp'].apply(to_uk_time)
            
            display_df = pd.DataFrame({
                'Station': latest_display['label'],
                'Level': latest_display['level'].round(2).astype(str) + "m",
                'Latest Reading': latest_display['timestamp'].dt.strftime("%d-%m-%Y @ %H:%M"),
                'station_id': latest_display['station_id']
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            # === CHARTS FOR EACH STATION ===
            for station in stations:
                st.write(f"### {station['label']}")
                hist = get_historical_data(station['id'], days=selected_days)
                if hist.empty:
                    st.write("No data.")
                    continue
                # Optional: inform user
                actual_days = (hist['Date'].max() - hist['Date'].min()).days if not hist.empty else 0
                if actual_days < selected_days - 1:
                    st.caption(f"Showing all available data ({actual_days} days). This station is relatively new.")
                chart_data = hist.copy()
                legend_items = [(REAL_LABEL, "#ad36eeff")]

                # === LEVEL PREDICTION ===
                if show_predictions:
                    if not chart_data.empty:
                        last_date = chart_data['Date'].max()
                        future_dates = pd.date_range(start=last_date, periods=3, freq='D')
                        future_df = pd.DataFrame({
                            'Date': future_dates,
                            'Level (metres)': [None] * len(future_dates),
                            'Type': ['Measured Level'] * len(future_dates)
                        })
                        chart_data = pd.concat([chart_data, future_df], ignore_index=True)

                # Rain
                rain_df = get_rainfall_data(station['id'], days=selected_days)
                if show_rain and not rain_df.empty:
                    chart_data = pd.concat([chart_data, rain_df], ignore_index=True)
                    legend_items.append((" Rainfall", "lightblue"))

                # === MAIN LEVEL LINE ===
                level_line = alt.Chart(chart_data).mark_line(strokeWidth=4).encode(
                    x=alt.X('Date:T', title='Date', axis=alt.Axis(format='%b %d', tickCount=14)),
                    y=alt.Y('Level (metres):Q', axis=alt.Axis(title='Level (m)', titleColor='white')),
                    color=alt.Color('Type:N', scale=alt.Scale(domain=[x[0] for x in legend_items], range=[x[1] for x in legend_items]), legend=None),
                    strokeDash=alt.condition(alt.datum.Type == REAL_LABEL, alt.value([0]), alt.value([6,4])),
                    tooltip=[
                        alt.Tooltip('Date:T', title='Date', format='%d-%m-%Y @ %H:%M'),
                        alt.Tooltip('Level (metres):Q', title='Level (metres)', format='.3f')
                    ]
                ).transform_filter(alt.FieldOneOfPredicate(field='Type', oneOf=[x[0] for x in legend_items if 'Rainfall' not in x[0]]))

                # === GOOD FISHING BAND ===
                if show_sweet_spot:
                    station_id = station['id']
                    if station_id in RULES and "good_min" in RULES[station_id] and "good_max" in RULES[station_id]:
                        good_min = RULES[station_id]["good_min"]
                        good_max = RULES[station_id]["good_max"]
                        min_date = chart_data['Date'].min()
                        max_date = chart_data['Date'].max()
                        band_data = pd.DataFrame({
                            'Date': [min_date, max_date],
                            'ymin': [good_min, good_min],
                            'ymax': [good_max, good_max]
                        })
                        good_band = alt.Chart(band_data).mark_rect(
                            color='#34d399', opacity=0.18
                        ).encode(
                            x=alt.X('Date:T'),
                            y=alt.Y('ymin:Q'),
                            y2=alt.Y2('ymax:Q')
                        )
                        level_line = alt.layer(good_band, level_line)
                        legend_items.append(("Good Fishing", "#22c55e"))

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
                    st.map(pd.DataFrame([{"lat": station['lat'], "lon": station['lon']}]), zoom=13, use_container_width=True)