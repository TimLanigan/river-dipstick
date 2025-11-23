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
from dotenv import load_dotenv
import os

load_dotenv()
DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@db:5432/river_levels_db'

# === IMPORT STATIONS (RELOAD ON EVERY RUN) ===
from river_reference import load_stations
STATIONS = load_stations()

# === LOAD RULES FOR COLOUR CODING (Docker + raw VPS compatible) ===
from pathlib import Path

RULES_FILE = Path("/app/data/rules.json")        # Docker path
if not RULES_FILE.exists():
    RULES_FILE = Path("/home/river_levels_app/rules.json")  # raw VPS fallback

try:
    with open(RULES_FILE, "r") as f:
        RULES = json.load(f)
except FileNotFoundError:
    RULES = {}
    st.warning("rules.json not found — colour-coding disabled.")

# === PAGE SELECTOR ===
page = st.sidebar.selectbox("Page", ["Dashboard", "About"])

# Fix narrow layout after refresh — THIS IS THE LINE YOU NEED
st.set_page_config(layout="wide")

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
    col1, col2 = st.columns([8, 1])
    with col1:
        st.title("About River Dipstick")

    st.write("This app is designed to answer the eternal question...")
    st.markdown("*Will the river be good for fly fishing tomorrow?*")
    st.write("The features described below are experimental but will improve over time as more data is collected.")
    st.write("My primary focus is rivers in the NW UK, the rivers I fish or hope to fish. This keeps cost and complexity low.")
    st.subheader("Colour Coding")
    st.write("The colours in each river table indicate if the conditions are good for fly fishing near that measurement station at the current time.")
    st.markdown(':red[Bad levels, stay at home]')
    st.markdown(':yellow[It might be worth a cast]')
    st.markdown(':green[Pull a sicky and go fishing!!]')
    st.write("Colour coding is only added to beats I know about, crowd-sourcing will come next.")
    st.subheader("Predicted Levels")
    st.write("Machine Learning (ML) is used to predict the river level in 24 hours.")
    st.write("The current model uses at least 1 years worth of Level and Rainfall data to make a prediction for each measurement station")
    st.write("To see the ML predictions, toggle 'Predictions' on the Dashboard.")
    st.markdown(':blue[The blue line is real data from the Environment Agency]')
    st.markdown(':grey[The grey line shows how the prediction model performed over the previous 7 days]')
    st.markdown(':violet[Violet is the 24 hr river level prediction]')
    st.subheader("Rainfall")
    st.write("Toggle 'Rain' to view how much rain has fallen near the measurement station over the last 7 days.")
    st.subheader("etc")
    st.write("Future plans: improved ML, more rivers and more fishing.")
    st.write("All data source from: [Environment Agency API](https://environment.data.gov.uk/flood-monitoring/doc/reference)")
    st.write("Built using [streamlit.io](https://streamlit.io) & vibe coded by tim & grok.")
    st.write("Finally, when I go fishing I take pictures and talk about it here: [downstream blog](https://downstreamblog.uk).")

# === PAGE: DASHBOARD ===
else:
    # === RESPONSIVE HEADER: TITLE + TAGLINE (CENTRED) ===
    st.markdown(
    """
    <div style="text-align: center; padding: 10px 0;">
        <div style="font-size: 2.8rem; font-weight: bold; color: white;">
            River Dipstick
        </div>
        <div style="font-size: 1.4rem; color: #ff6b6b; font-style: italic; margin-top: -10px;">
            a flyfisher's wet dream
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
        show_rain = st.toggle("Rain", value=False, key="show_rain", help="Show rainfall from nearest EA gauge")
    with col_t3:
        show_map = st.toggle("Map", value=False, key="show_map")
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
        # Custom tab labels
        tab_labels = {
            "Eden": "Eden",
            "Ribble": "Ribble",
            "Lune": "Lune"
        }
        tabs = st.tabs([tab_labels.get(r, r) for r in STATIONS.keys()])

        for i, river in enumerate(STATIONS.keys()):
            with tabs[i]:
                river_stations = STATIONS[river]
                
                # === SORT STATIONS SOURCE TO SEA (CUSTOM PER RIVER) ===
                if river == 'Eden':
                    # Eden: source south, sea north - sort lat ascending
                    river_stations = sorted(river_stations, key=lambda s: s.get('lat', 0))
                else:
                    # Ribble/Lune: source north, sea south - sort lat descending
                    river_stations = sorted(river_stations, key=lambda s: s.get('lat', 0), reverse=True)

                # Map STATIONS key → DB river name
                db_river_name = river
                river_df = df[df['river'] == db_river_name].copy()
                if not river_df.empty:
                    # === GET LATEST READING PER STATION ===
                    river_df['timestamp'] = pd.to_datetime(river_df['timestamp'])
                    latest_df = river_df.loc[river_df.groupby('station_id')['timestamp'].idxmax()]
                    
                    # Reindex in source-to-sea order
                    latest_df = latest_df.set_index('station_id').reindex([s['id'] for s in river_stations]).reset_index()             
                    latest_df = latest_df.rename(columns={'river': 'River', 'label': 'Station', 'timestamp': 'Latest Reading'})
                    latest_df = latest_df[['River', 'Station', 'level', 'Latest Reading', 'station_id']]

                    # === FORMAT TIMESTAMP BEFORE BADGE ===
                    def format_time(x):
                        if pd.isna(x):
                            return "No data"
                        return x.strftime('%d-%m-%Y @ %H:%M')

                    # === ADD "NO RECENT DATA" BADGE IF >2H OLD ===
                    now = datetime.now(UTC)
                    for idx, row in latest_df.iterrows():
                        if pd.notna(row['Latest Reading']):
                            ts = pd.to_datetime(row['Latest Reading'])
                            hours_ago = (now - ts).total_seconds() / 3600
                            if hours_ago > 2:
                                latest_df.loc[idx, 'Latest Reading'] = f"{row['Latest Reading']} ⚠️"

                    # Safe format
                    def safe_time_format(x):
                        return x.strftime('%d-%m-%Y @ %H:%M') if pd.notna(x) and '⚠️' not in str(x) else str(x)

                    styled_river = latest_df.style.apply(apply_styles, axis=1).format({
                        "level": "{:.2f}m",
                        "Latest Reading": safe_time_format
                    })
                    styled_river = styled_river.hide(subset=['station_id'], axis="columns")
                    st.dataframe(styled_river, hide_index=True)
                else:
                    st.write(f"No current data for {river}")

                    # ... your full chart logic ...
                # === STATION CHARTS ===
                for station in river_stations:  # Use sorted river_stations for charts too
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
                            title=f"Mega chart:"
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

# Buy Me a Coffee
st.markdown(
    """
    <div style="text-align: left; margin: 20px 0;">
    Use the menu in the top right hand corner to access the about page for more info about the site and how the features work.<br><br>
    If I've helped you catch a fish, please consider buying me a coffee to help keep me and the servers running sweet!<br><br>
        <a href="https://buymeacoffee.com/riverdipstick" target="_blank">
            <img src="https://img.buymeacoffee.com/button-api/?text=Buy me a coffee&emoji=☕&slug=riverdipstick&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=FFFFFF" height="42">
        </a>
    </div>
    """,
    unsafe_allow_html=True
)

# About + Data Source
#st.markdown(
#    """
#    <div style="text-align: left; font-size: 0.9rem; color: #888;">
#        <a href="https://environment.data.gov.uk/flood-monitoring/doc/reference" target="_blank">Data source: Environment Agency API</a> •
#        Built with <a href="https://streamlit.io" target="_blank">Streamlit</a> & 
#        <a href="?page=about" style="color: #888;">vibe coded by tim and grok</a>
#    </div>
#    """,
#    unsafe_allow_html=True
#)