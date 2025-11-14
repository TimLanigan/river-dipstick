import streamlit as st
import sqlite3
import pandas as pd
import time
import json
import altair as alt  # For advanced charts
from datetime import datetime, timedelta, UTC  # Added UTC
st.set_page_config(page_title="dipstick", page_icon="/home/river_levels_app/favicon.png")  # Replace with your favicon path or URL

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
    start_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    query = """
        SELECT timestamp, level
        FROM readings
        WHERE station_id = ? AND timestamp >= ?
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.rename(columns={'level': 'Level (metres)'})
    df['Type'] = 'Historical'
    return df

def get_predictions(station_id, days=7):
    conn = sqlite3.connect(DB_FILE)
    start_date = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    query = "SELECT predicted_for, predicted_level FROM predictions WHERE station_id = ? AND predicted_for >= ? ORDER BY predicted_for"
    df = pd.read_sql_query(query, conn, params=(station_id, start_date))
    conn.close()
    if not df.empty:
        df['predicted_for'] = pd.to_datetime(df['predicted_for'])
        df = df.rename(columns={'predicted_for': 'Date', 'predicted_level': 'Level (metres)'})
        df['Type'] = 'Predicted'
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
    st.title("About River Dipstick")
    st.write("This app is designed to answer the eternal question...")
    st.markdown("*Will the river be good for fly fishing tomorrow?*")
    st.write("The colours in each table indicate if the level is good for Fly Fishing")
    st.markdown(''':red[Bad levels, stay at home]''')
    st.markdown(''':yellow[It might be worth a cast]''')
    st.markdown(''':green[Pull a sicky and go fishing!!]''')
    st.write("Colour coding is based on real world experience, crowdsourced from local fisherman.")
    st.write("Machine Learning (ML) is used to predict the river level in 24 hours.")
    st.write("To see the ML predictions, open the top right menu and toggle 'show predictions'")
    st.markdown(''':blue[The blue line is real data from the Environment Ageny]''')
    st.markdown(''':grey[The grey area shows how the prediction model performed over the previous 7 days]''')
    st.markdown(''':violet[Violet is the 24 hr prediction]''')
    st.write("Future plans: full rainfall integration, improved ML, more rivers and more fishing.")
else:
    st.title("River Dipstick")
    st.write("Powered by gut instinct & machine learning")

    # Toggle for predictions
    show_predictions = st.sidebar.toggle("Show Predictions", value=False)

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

                    for station in river_stations:
                        st.write(f"### {station['label']}")
                        hist_df = get_historical_data(station['id'])
                        if not hist_df.empty:
                            hist_df = hist_df.reset_index().rename(columns={'timestamp': 'Date'})
                            hist_df['Type'] = 'Real'
                            chart_data = hist_df
                            legend = None  # No legend by default
                            if show_predictions:
                                pred_df = get_predictions(station['id'])
                                if not pred_df.empty:
                                    pred_df = pred_df.reset_index().rename(columns={'predicted_for': 'Date'})
                                    # Split past and future
                                    now = datetime.now(tz=UTC)  # Make aware
                                    past_pred = pred_df[pred_df['Date'] < now].copy()
                                    future_pred = pred_df[pred_df['Date'] >= now].copy()
                                    past_pred['Type'] = 'Past'
                                    future_pred['Type'] = 'Future'
                                    chart_data = pd.concat([chart_data, past_pred, future_pred])
                                legend = alt.Legend()  # Show legend when predictions on
                            alt_chart = alt.Chart(chart_data).mark_line().encode(
                                x=alt.X('Date:T', title='Date'),
                                y=alt.Y('Level (metres):Q', title='Level (metres)'),
                                color=alt.Color('Type:N', scale=alt.Scale(domain=['Real', 'Past', 'Future'], range=['blue', 'grey', 'violet']), legend=legend),
                                strokeDash=alt.condition(
                                    alt.datum.Type == 'Real',
                                    alt.value([0]),  # Solid for historical
                                    alt.value([5, 5])  # Dashed for predictions
                                ),
                                opacity=alt.condition(
                                    alt.datum.Type == 'Real',
                                    alt.value(1.0),  # Full opacity for historical
                                    alt.value(0.4)   # More translucent for predictions
                                ),
                                tooltip=['Date', 'Level (metres)', 'Type']
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