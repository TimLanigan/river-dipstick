import streamlit as st
import sqlite3
import pandas as pd
import time
import json
import altair as alt  # For advanced charts
from datetime import datetime, timedelta

DB_FILE = '/home/river_levels_app/river_levels.db'
RULES_FILE = '/home/river_levels_app/rules.json'

# Load rules from JSON
try:
    with open(RULES_FILE, 'r') as f:
        RULES = json.load(f)
except FileNotFoundError:
    RULES = {}
    st.warning("rules.json not found—color-coding disabled.")

def get_latest_readings():
    conn = sqlite3.connect(DB_FILE)
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
    conn = sqlite3.connect(DB_FILE)
    start_date = (datetime.now() - timedelta(days=days)).isoformat()
    query = """
        SELECT timestamp, level
        FROM readings
        WHERE station_id = ? AND timestamp >= ?
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')
    return df

def get_predictions(station_id):
    conn = sqlite3.connect(DB_FILE)
    query = "SELECT predicted_for, predicted_level FROM predictions WHERE station_id = ? ORDER BY predicted_for"
    df = pd.read_sql_query(query, conn, params=(station_id,))
    conn.close()
    if not df.empty:
        df['predicted_for'] = pd.to_datetime(df['predicted_for'])
        df = df.set_index('predicted_for')
    return df

# Optional auto-refresh
REFRESH_INTERVAL = 60
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > REFRESH_INTERVAL:
    st.session_state.last_refresh = time.time()
    st.rerun()

# Sidebar navigation
page = st.sidebar.selectbox("Page", ["Dashboard", "About"])

if page == "About":
    st.title("About the North West River Dipstick")
    st.write("This app provides river level data & forecasts for a couple of rivers in the North West UK.")
    st.write("Data is pulled from various sources, AI predicts the river level in 24 hours.")
    st.write("River Levels are displayed in metres. The colour coding is the height of the river in that moment, with fly fishing in mind")
    st.write("red = bad level, stay at home")
    st.write("yellow = might be worth a cast")
    st.write("green = perfect level for fly fishing")
    st.write("Future plans: Rainfall integration, more rivers, improved Machine Learning. And going fishing instead of coding.")
else:
    st.title("NW River Dipstick")
    st.write("Powered by gut instinct & AI.")

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

        from river_reference import STATIONS

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

                    st.subheader(f"Historical Levels (Last 7 Days) - {river}")
                    for station in river_stations:
                        st.write(f"### {station['label']}")
                        hist_df = get_historical_data(station['id'])
                        if not hist_df.empty:
                            chart_data = hist_df.reset_index().rename(columns={'timestamp': 'Time', 'level': 'Level'})
                            chart_data['Type'] = 'Historical'
                            pred_df = get_predictions(station['id'])
                            if not pred_df.empty:
                                pred_df = pred_df.reset_index().rename(columns={'predicted_for': 'Time', 'predicted_level': 'Level'})
                                pred_df['Type'] = 'Predicted'
                                combined = pd.concat([chart_data, pred_df])
                            else:
                                combined = chart_data
                            alt_chart = alt.Chart(combined).mark_line().encode(
                                x='Time:T',
                                y='Level:Q',
                                color='Type:N',
                                strokeDash=alt.condition(alt.datum.Type == 'Predicted', alt.value([5, 5]), alt.value([0]))
                            ).properties(
                                width=700,
                                height=300
                            )
                            st.altair_chart(alt_chart, use_container_width=True)
                        else:
                            st.write("No historical data available.")
    else:
        st.write("No data available yet. Run the collection script first.")
    st.write("Data refreshed:  " + pd.Timestamp.now().strftime('%d-%m-%Y @ %H:%M'))
    st.write("Data source: [Environment Agency API](https://environment.data.gov.uk/flood-monitoring/doc/reference) ")
    st.write("Built using [streamlit.io](https://streamlit.io) & vibe coded by tim.")