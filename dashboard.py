import streamlit as st
import sqlite3
import pandas as pd
import time
import json  # New: For loading rules from JSON
from datetime import datetime, timedelta  # Added for historical queries

DB_FILE = '/home/river_levels_app/river_levels.db'
RULES_FILE = '/home/river_levels_app/rules.json'  # Absolute path to JSON

# Load rules from JSON (do this once outside functions)
try:
    with open(RULES_FILE, 'r') as f:
        RULES = json.load(f)
except FileNotFoundError:
    RULES = {}  # Fallback if file missing
    st.warning("rules.json not found—color-coding disabled.")

def get_latest_readings():
    """Query DB for the latest reading per station."""
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
    # Convert timestamp to datetime for formatting
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def get_historical_data(station_id, days=7):
    """Query DB for historical levels for a station (last N days)."""
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

# Optional auto-refresh
REFRESH_INTERVAL = 60  # seconds
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > REFRESH_INTERVAL:
    st.session_state.last_refresh = time.time()
    st.rerun()

st.title("NW River Dipstick")
st.write("Powered by gut instinct & AI.")

df = get_latest_readings()
if not df.empty:
    # Updated: Styling function now uses loaded RULES
    def apply_styles(row):
        styles = [''] * len(row)
        level_idx = row.index.get_loc('level')
        station_id = row['station_id']
        level = row['level']
        
        if station_id in RULES:
            for rule in RULES[station_id]:
                min_val = rule['min']
                max_val = rule['max'] if rule['max'] is not None else float('inf')
                if min_val <= level < max_val:  # Use < for upper bound to avoid overlap
                    color = rule['color']
                    # Map color to CSS (adjust for readability)
                    if color == 'red':
                        styles[level_idx] = 'background-color: red; color: white;'
                    elif color == 'yellow':
                        styles[level_idx] = 'background-color: yellow; color: black;'
                    elif color == 'lightgreen':
                        styles[level_idx] = 'background-color: lightgreen; color: black;'
                    # Add more color mappings if needed
                    break  # Stop after first match
        
        return styles

    # Custom orders: station IDs from source to sea
    RIBBLE_ORDER = ['710151', '710102', '710103', '710301', '710305', '713056', '713040', '713354']
    EDEN_ORDER = ['760101', '760112', '760115', '760502', '762505', '765512', '762540']

    # Split into two DataFrames
    df_ribble = df[df['river'] == 'Ribble'].copy()
    df_eden = df[df['river'] == 'Eden'].copy()

    # Sort Ribble by custom order
    if not df_ribble.empty:
        df_ribble['sort_order'] = pd.Categorical(df_ribble['station_id'], categories=RIBBLE_ORDER, ordered=True)
        df_ribble = df_ribble.sort_values('sort_order').drop('sort_order', axis=1)
        # Rename columns as requested
        df_ribble = df_ribble.rename(columns={'river': 'River', 'label': 'Station', 'timestamp': 'Latest Reading'})
        # Reorder columns (keep station_id for styling, but hide later)
        df_ribble = df_ribble[['River', 'Station', 'level', 'Latest Reading', 'station_id']]
        st.subheader("River Ribble")
        styled_ribble = df_ribble.style.apply(apply_styles, axis=1).format({"level": "{:.2f}m", "Latest Reading": "{:%d-%m-%Y @ %H:%M}"})
        styled_ribble = styled_ribble.hide(subset=['station_id'], axis="columns")
        st.dataframe(styled_ribble, hide_index=True)

        # Add historical chart for each Ribble station
        st.subheader("Historical Levels (Last 7 Days) - Ribble")
        for index, row in df_ribble.iterrows():
            st.write(f"### {row['Station']}")
            hist_df = get_historical_data(row['station_id'])
            if not hist_df.empty:
                st.line_chart(hist_df['level'])
            else:
                st.write("No historical data available.")

    # Sort Eden by custom order
    if not df_eden.empty:
        df_eden['sort_order'] = pd.Categorical(df_eden['station_id'], categories=EDEN_ORDER, ordered=True)
        df_eden = df_eden.sort_values('sort_order').drop('sort_order', axis=1)
        # Rename columns as requested
        df_eden = df_eden.rename(columns={'river': 'River', 'label': 'Station', 'timestamp': 'Latest Reading'})
        # Reorder columns (keep station_id for styling, but hide later)
        df_eden = df_eden[['River', 'Station', 'level', 'Latest Reading', 'station_id']]
        st.subheader("River Eden")
        styled_eden = df_eden.style.apply(apply_styles, axis=1).format({"level": "{:.2f}m", "Latest Reading": "{:%d-%m-%Y @ %H:%M}"})
        styled_eden = styled_eden.hide(subset=['station_id'], axis="columns")
        st.dataframe(styled_eden, hide_index=True)

        # Add historical chart for each Eden station
        st.subheader("Historical Levels (Last 7 Days) - Eden")
        for index, row in df_eden.iterrows():
            st.write(f"### {row['Station']}")
            hist_df = get_historical_data(row['station_id'])
            if not hist_df.empty:
                st.line_chart(hist_df['level'])
            else:
                st.write("No historical data available.")

else:
    st.write("No data available yet. Run the collection script first.")
st.write("Key:")
st.write("red = bad level, stay at home")
st.write("yellow = might be worth a cast")
st.write("green = perfect level for fly fishing")

st.write("Data refreshed:  " + pd.Timestamp.now().strftime('%d-%m-%Y @ %H:%M'))
st.write("Data source: [Environment Agency API](https://environment.data.gov.uk/flood-monitoring/doc/reference) ")
st.write("Built using [streamlit.io](https://streamlit.io) & vibe coded by tim.")