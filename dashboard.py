#!/usr/bin/env python3
"""
dashboard.py - River Dipstick Dashboard
Mega Graph: Level + Rain + Predictions
"""

import streamlit as st
import psycopg2
import pandas as pd
import time
import json
import altair as alt
from datetime import datetime, timedelta, UTC

# === IMPORT STATIONS ===
from river_reference import STATIONS

st.set_page_config(
    page_title="River Dipstick",
    page_icon="static/logo.png",  # Relative path
    layout="wide"
)
CONNECTION_STRING = 'dbname=river_levels_db user=river_user password=***REMOVED*** host=localhost'
RULES_FILE = '/home/river_levels_app/rules.json'

# Load rules from JSON
try:
    with open(RULES_FILE, 'r') as f:
        RULES = json.load(f)
except FileNotFoundError:
    RULES = {}
    st.warning("rules.json not found—color-coding disabled.")

# === PAGE SELECTOR ===
page = st.sidebar.selectbox("Page", ["Dashboard", "About"])

# === DATABASE FUNCTIONS ===
def get_latest_readings():
    conn = psycopg2.connect(CONNECTION_STRING)
    query = """
        SELECT station_id, river, label, level, timestamp
        FROM readings
        WHERE id IN (
            SELECT MAX(id) FROM readings GROUP BY station_id
        )
        ORDER BY river, label
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def get_historical_data(station_id, days=7):
    conn = psycopg2.connect(CONNECTION_STRING)
    start_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    query = """
        SELECT timestamp, level
        FROM readings
        WHERE station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.rename(columns={'level': 'Level (metres)'})
        df['Type'] = 'Real'
    return df

def get_predictions(station_id, days=7):
    conn = psycopg2.connect(CONNECTION_STRING)
    start_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    query = "SELECT predicted_for, predicted_level FROM predictions WHERE station_id = %s AND predicted_for >= %s ORDER BY predicted_for"
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    if not df.empty:
        df['predicted_for'] = pd.to_datetime(df['predicted_for'])
        df = df.rename(columns={'predicted_for': 'Date', 'predicted_level': 'Level (metres)'})
        df['Type'] = 'Predicted'
    return df

def get_rainfall_data(level_station_id, days=7):
    conn = psycopg2.connect(CONNECTION_STRING)
    start_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    query = """
        SELECT timestamp, rainfall_mm
        FROM rainfall_readings
        WHERE level_station_id = %s AND timestamp >= %s
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(level_station_id, start_date))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.rename(columns={'timestamp': 'Date', 'rainfall_mm': 'Rainfall (mm)'})
        df['Type'] = 'Rainfall'
    return df

# === AUTO-REFRESH ===
REFRESH_INTERVAL = 60
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()
if time.time() - st.session_state.last_refresh > REFRESH_INTERVAL:
    st.session_state.last_refresh = time.time()
    st.rerun()

# === PAGE: ABOUT ===
if page == "About":
    col1, col2 = st.columns([1, 8])
    with col1:
        st.image("/home/river_levels_app/favicon.png", width=60)
    with col2:
        st.title("About River Dipstick")
    st.write("This app is designed to answer the eternal question...")
    st.markdown("*Will the river be good for fly fishing tomorrow?*")
    st.write("The colours in each table indicate if the conditions are good for fly fishing for trout")
    st.markdown(':red[Bad levels, stay at home]')
    st.markdown(':yellow[It might be worth a cast]')
    st.markdown(':green[Pull a sicky and go fishing!!]')
    st.write("Colour coding is based on real world experience, crowdsourced from local fisherman.")
    st.write("Machine Learning (ML) is used to predict the river level in 24 hours.")
    st.write("To see the ML predictions, toggle 'Predictions' on the Dashboard.")
    st.markdown(':blue[The blue line is real data from the Environment Agency]')
    st.markdown(':grey[The grey area shows how the prediction model performed over the previous 7 days]')
    st.markdown(':violet[Violet is the 24 hr prediction]')
    st.write("Future plans: full rainfall integration, improved ML, more rivers and more fishing.")
    st.write("Data source: [Environment Agency API](https://environment.data.gov.uk/flood-monitoring/doc/reference)")
    st.write("Built using [streamlit.io](https://streamlit.io) & vibe coded by tim.")

# === PAGE: DASHBOARD ===
else:
    # === RESPONSIVE HEADER: TITLE LEFT, LOGO BASE64 (MOBILE + DESKTOP) ===
    try:
        with open("static/logo_base64.txt", "r") as f:
            logo_base64 = f.read().strip()
    except FileNotFoundError:
        logo_base64 = ""  # Fallback

    st.markdown(
        f"""
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            flex-wrap: nowrap;
        ">
            <div style="font-size: 2rem; font-weight: bold; white-space: nowrap;">
                River Dipstick
            </div>
            <div style="margin-left: 20px; flex-shrink: 0;">
                <img src="data:image/png;base64,{logo_base64}" width="80" style="height: auto;">
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # === TOGGLES ===
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        show_predictions = st.toggle("Predictions", value=False, key="show_predictions")
    with col_t2:
        show_map = st.toggle("Map", value=False, key="show_map")
    with col_t3:
        show_rain = st.toggle("Rain", value=False, key="show_rain", help="Show rainfall from nearest EA gauge")

    # === DATA ===
    df = get_latest_readings()

    if not df.empty:
        def apply_styles(row):
            styles = [''] * len(row)
            level_idx = row.index.get_loc('level')
            station_id = row['station_id']
            level = row['level']
            if station_id in RULES:
                for rule in RULES[station_id]:
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

        # === RIVER TABS ===
        tabs = st.tabs(list(STATIONS.keys()))

        for i, river in enumerate(STATIONS.keys()):
            with tabs[i]:
                river_stations = STATIONS[river]
                river_df = df[df['river'] == river].copy()
                if not river_df.empty:
                    river_df = river_df.set_index('station_id').reindex([s['id'] for s in river_stations]).reset_index()
                    river_df = river_df.rename(columns={'river': 'River', 'label': 'Station', 'timestamp': 'Latest Reading'})
                    river_df = river_df[['River', 'Station', 'level', 'Latest Reading', 'station_id']]
                    styled_river = river_df.style.apply(apply_styles, axis=1).format({"level": "{:.2f}m", "Latest Reading": "{:%d-%m-%Y @ %H:%M}"})
                    styled_river = styled_river.hide(subset=['station_id'], axis="columns")
                    st.dataframe(styled_river, hide_index=True)

                # === STATION CHARTS ===
                for station in river_stations:
                    st.write(f"### {station['label']}")
                    hist_df = get_historical_data(station['id'])
                    if not hist_df.empty:
                        hist_df = hist_df.reset_index().rename(columns={'timestamp': 'Date'})
                        hist_df['Type'] = 'Real'
                        chart_data = hist_df.copy()
                        legend = None

                        # Predictions
                        if show_predictions:
                            pred_df = get_predictions(station['id'])
                            if not pred_df.empty:
                                pred_df = pred_df.reset_index().rename(columns={'predicted_for': 'Date'})
                                now = datetime.now(tz=UTC)
                                past_pred = pred_df[pred_df['Date'] < now].copy()
                                future_pred = pred_df[pred_df['Date'] >= now].copy()
                                past_pred['Type'] = 'Past'
                                future_pred['Type'] = 'Future'
                                chart_data = pd.concat([chart_data, past_pred, future_pred])
                                legend = alt.Legend()

                        # Rainfall
                        rain_df = get_rainfall_data(station['id'])
                        if show_rain and not rain_df.empty:
                            rain_df['Date'] = rain_df['Date'].dt.tz_localize(None)
                            chart_data = pd.concat([chart_data, rain_df])

                        # === MEGA GRAPH (FIXED LAYER) ===
                        base = alt.Chart(chart_data).encode(x=alt.X('Date:T', title='Date'))

                                                # Level line (BOLD FOREGROUND)
                        level_chart = base.mark_line(
                            strokeWidth=3  # ← Thicker line
                        ).encode(
                            y=alt.Y('Level (metres):Q', axis=alt.Axis(title='Level (m)', titleColor='blue')),
                            color=alt.Color('Type:N', scale=alt.Scale(domain=['Real', 'Past', 'Future'], range=['blue', 'grey', 'violet']), legend=legend),
                            strokeDash=alt.condition(
                                alt.datum.Type == 'Real',
                                alt.value([0]),
                                alt.value([5, 5])
                            ),
                            tooltip=['Date', 'Level (metres)', 'Type']
                        ).transform_filter(alt.datum.Type != 'Rainfall')

                        # Rainfall bars (FADED BACKGROUND)
                        rain_chart = base.mark_bar(
                            color='lightblue',
                            opacity=0.25,  # ← 25% opacity = subtle background
                            size=6
                        ).encode(
                            y=alt.Y('Rainfall (mm):Q', axis=alt.Axis(title='Rain (mm)', titleColor='lightblue')),
                            tooltip=['Date', 'Rainfall (mm)']
                        ).transform_filter(alt.datum.Type == 'Rainfall')

                        # === BUILD LAYER CORRECTLY ===
                        if show_rain and not rain_df.empty:
                            mega_chart = (level_chart + rain_chart).resolve_scale(y='independent')
                        else:
                            mega_chart = level_chart

                        mega_chart = mega_chart.properties(
                            width=700,
                            height=300,
                            title=f"{station['label']} — Level + Rain + Predictions"
                        )
                        st.altair_chart(mega_chart, use_container_width=True)

                        # === MAP ===
                        if show_map and station.get('lat') and station.get('lon'):
                            st.map(pd.DataFrame([{"lat": station['lat'], "lon": station['lon']}]),
                                   zoom=11)
                            st.caption(f"Location: {station['label']} ({river})")

                    else:
                        st.write("No historical data available.")
    else:
        st.write("No data available yet. Run the collection script first.")

    # === FOOTER ===
    st.markdown("---")
    st.write(f"Data refreshed: {pd.Timestamp.now().strftime('%d-%m-%Y @ %H:%M')}")
    st.write("Data source: [Environment Agency API](https://environment.data.gov.uk/flood-monitoring/doc/reference)")
    st.write("Built using [streamlit.io](https://streamlit.io) & vibe coded by tim.")