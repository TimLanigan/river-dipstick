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
def load_css(file_path="style.css"):
    with open(file_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()
load_dotenv()

DB_PASS = os.getenv("DB_PASSWORD")
CONNECTION_STRING = f'postgresql://river_user:{DB_PASS}@wintermute-db:5432/river_levels_db'
REAL_LABEL = "Measured Level"

# UK Timezone helper
def to_uk_time(utc_dt):
    uk_tz = pytz.timezone('Europe/London')
    if isinstance(utc_dt, str):
        utc_dt = pd.to_datetime(utc_dt)
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.tz_localize('UTC')
    return utc_dt.tz_convert(uk_tz)

# === CUSTOM FAVICON SETUP ===
# Drop your new favicon as: app/static/favicon.png
# We set page_icon + inject <link> tags for maximum compatibility
# (tabs + bookmarks often need both).
FAVICON_PATH = "static/favicon.png"
page_icon = FAVICON_PATH if os.path.exists(FAVICON_PATH) else None

st.set_page_config(
    layout="wide",
    page_title="River Dipstick",
    page_icon=page_icon,
    initial_sidebar_state="collapsed"
)

st.markdown(
    f"""
    <link rel="icon" type="image/png" href="{FAVICON_PATH}">
    <link rel="shortcut icon" href="{FAVICON_PATH}">
    """,
    unsafe_allow_html=True
)

load_css("style.css")

# === SIDEBAR ===
with st.sidebar:
    st.markdown("""<h2 class="site-title">River Dipstick</h2>""", unsafe_allow_html=True)
    show_sweet_spot = st.toggle("Find G Spot", value=False, help="Highlight good fishing levels at key stations")
    show_predictions = st.toggle("Extend Chart", value=False, help="Use Mk1 eyeball to predict level")
    show_pressure = st.toggle("Pressure Trend", value=False, help="Show pressure history + forecast")
    show_rain = st.toggle("Rain History", value=False, help="Show historic rainfall on the chart")
    show_map = st.toggle("View maps", value=False, help="Show measuring station")
    st.markdown("---")
    days_options = [7, 14, 30, 60, 180, 365]
    selected_days = st.select_slider("Graph history (days)", options=days_options, value=7)

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

def get_pressure_data(river, days=selected_days):
    """Fetch historic + forecast pressure for a river (for dual-axis overlay)"""
    conn = psycopg2.connect(CONNECTION_STRING)
    start = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    df = pd.read_sql_query("""
        SELECT forecast_date, pressure_hpa
        FROM pressure_forecasts
        WHERE river = %s AND forecast_date >= %s
        ORDER BY forecast_date
    """, conn, params=(river, start))
    conn.close()
    
    if df.empty:
        return pd.DataFrame(columns=['Date', 'Pressure', 'Type'])
    
    df = df.copy()
    df = df.rename(columns={'forecast_date': 'Date', 'pressure_hpa': 'Pressure'})
    df['Date'] = pd.to_datetime(df['Date']).apply(to_uk_time)

    # Resample to hourly for a smooth continuous line (averages the dense 15-min readings)
    df = (
        df.set_index('Date')
          .resample('1h')
          .mean()
          .dropna()
          .reset_index()
    )

    df['Type'] = 'Pressure'
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
    all_rivers = sorted(STATIONS.keys())
    tab_list = all_rivers + ["About"]
    tabs = st.tabs(tab_list)

    for tab, river in zip(tabs, tab_list):
        with tab:
            if river == "About":
                # === ABOUT PAGE ===
                st.write("The **River Dipstick** is built by a Lancashire fly fisherman primarily for himself and other local legends.")

                st.markdown("### Features")
                st.write("- Access the sidebar by selecting the menu >> in the top lefthand corner of the site\n- Select 'Find G Spot' to highlight good fishing levels on selected charts, based on local wisdom (where it exists)\n- When viewing the 'Good Fishing Band' on a chart; remember... a falling river is always best\n- Select 'Extend Chart' for space to eyeball trends\n- Select 'Pressure Trend' to stitch together recent pressure readings with forecasted atmospheric pressure\n- Select 'Rainfall History' to view recent rainfall data\n- Select 'Maps' to see where the measuring station is located\n- Use the 'Graph History' slider to show more or less data on the charts\n- **Don't spend too long looking at data, if in doubt... go fishing** 😊")

                st.markdown("### Data Sources")
                st.write("- **Environment Agency (EA)** – Real-time river levels and rainfall")
                st.write("- **SEPA** – Real-time river levels (Scotland)")
                st.write("- **Open-Meteo** – Atmospheric pressure observations and forecasts")

                st.markdown("### Blog")
                st.write("Fishing reports, river notes, and the occasional adventure at [downstreamblog.uk](https://downstreamblog.uk/)")

                st.markdown("### Tech")
                st.write("100% open source - [https://github.com/TimLanigan/river-dipstick]")
                st.write("Built with Streamlit • PostgreSQL • Altair • Docker")

                st.markdown("### Feedback")

                st.write("If something’s broken or you’ve got a suggestion for new feature that might be useful for us fishing nerds, let me know.")

                st.link_button(
                    "Give Feedback",
                    "https://tally.so/r/rjvdK2",
                    use_container_width=True
                )
                continue

            # === NORMAL RIVER TAB ===
            stations = STATIONS.get(river, [])
            river_df = df[df['river'] == river].copy()
            if river_df.empty:
                st.write("No data.")
                continue

            # Table
            latest = river_df.loc[river_df.groupby('station_id')['timestamp'].idxmax()]
            latest = latest.set_index('station_id').reindex([s['id'] for s in stations]).dropna(subset=['river']).reset_index()

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

                actual_days = (hist['Date'].max() - hist['Date'].min()).days if not hist.empty else 0
                if actual_days < selected_days - 1:
                    st.caption(f"Showing all available data ({actual_days} days). This station is relatively new.")

                chart_data = hist.copy()
                legend_items = [(REAL_LABEL, "#ad36eeff")]

                # Rain
                rain_df = get_rainfall_data(station['id'], days=selected_days)
                if show_rain:
                    if not rain_df.empty:
                        chart_data = pd.concat([chart_data, rain_df], ignore_index=True)
                        legend_items.append((" Rainfall", "lightblue"))
                    else:
                        st.caption("No rainfall data available for this station")

                # Pressure Trend
                pressure_df = get_pressure_data(river, days=selected_days)
                if show_pressure:
                    if not pressure_df.empty:
                        chart_data = pd.concat([chart_data, pressure_df], ignore_index=True)
                        legend_items.append(("Pressure", "#9ca3af"))
                    else:
                        st.caption("No pressure data available yet for this river")

                # === EXTEND CHART (Eyeball Future) ===
                if show_predictions and not chart_data.empty:
                    last_date = chart_data['Date'].max()
                    future_dates = pd.date_range(start=last_date, periods=2, freq='d')
                    future_df = pd.DataFrame({
                        'Date': future_dates,
                        'Level (metres)': [None] * len(future_dates),
                        'Type': ['Eyeball Future'] * len(future_dates)
                    })
                    chart_data = pd.concat([chart_data, future_df], ignore_index=True)
                    legend_items.append(("Eyeball Future", "#9e9e9e"))

                # === CORE LEVEL LINE (the important fishing signal) ===
                # Mild smoothing via Altair transform_window to reduce gauge jitter
                # (especially visible on low-flow stations like Great Musgrave Bridge)
                # while keeping the overall shape and timing honest.
                # frame=[-2, 2] = 5-point centered moving average (light touch).
                # Tooltips still show the raw 'Level (metres)' value for accuracy.
                LEVEL_SMOOTH_WINDOW = [-2, 2]

                level_line = (
                    alt.Chart(chart_data)
                    .transform_filter(alt.datum.Type == REAL_LABEL)
                    .transform_window(
                        smoothed_level='mean(Level (metres))',
                        frame=LEVEL_SMOOTH_WINDOW
                    )
                    .mark_line(strokeWidth=4)
                    .encode(
                        x=alt.X('Date:T', title='Date', axis=alt.Axis(format='%b %d', tickCount=14)),
                        y=alt.Y('smoothed_level:Q', axis=alt.Axis(title='Level (m)', titleColor='white')),
                        color=alt.value("#ad36eeff"),
                        tooltip=[
                            alt.Tooltip('Date:T', title='Date', format='%d-%m-%Y @ %H:%M'),
                            alt.Tooltip('Level (metres):Q', title='Level (metres) (raw)', format='.3f')
                        ]
                    )
                )

                # === GOOD FISHING BAND (static from rules.json) ===
                band_layer = None
                if show_sweet_spot:
                    sid = station['id']
                    if sid in RULES:
                        rule = RULES[sid]
                        if 'good_min' in rule and 'good_max' in rule:
                            band_df = pd.DataFrame({
                                'Date': [chart_data['Date'].min(), chart_data['Date'].max()],
                                'ymin': [rule['good_min'], rule['good_min']],
                                'ymax': [rule['good_max'], rule['good_max']]
                            })
                            band_layer = alt.Chart(band_df).mark_rect(
                                color='#22c55e',   # nice green
                                opacity=0.16
                            ).encode(
                                x='Date:T',
                                y=alt.Y('ymin:Q'),
                                y2=alt.Y2('ymax:Q')
                            )
                            legend_items.append(("Good Band", "#22c55e"))

                # Start building the final layered chart
                if band_layer is not None:
                    chart = alt.layer(band_layer, level_line)
                else:
                    chart = level_line

                # === PRESSURE TREND (dual Y-axis on the right) ===
                if show_pressure and not pressure_df.empty:
                    pressure_line = alt.Chart(chart_data).mark_line(
                        strokeWidth=1.8,
                        opacity=0.65
                    ).encode(
                        x=alt.X('Date:T'),
                        y=alt.Y('Pressure:Q',
                                axis=alt.Axis(title='Pressure (hPa)', titleColor='#9ca3af', orient='right'),
                                scale=alt.Scale(zero=False)
                        ),
                        color=alt.value("#9ca3af"),
                        tooltip=[
                            alt.Tooltip('Date:T', title='Date', format='%d-%m-%Y @ %H:%M'),
                            alt.Tooltip('Pressure:Q', title='Pressure (hPa)', format='.1f')
                        ]
                    ).transform_filter(alt.datum.Type == 'Pressure')

                    chart = alt.layer(chart, pressure_line).resolve_scale(y='independent')

                # === RAINFALL BARS (dual axis, existing behaviour) ===
                if show_rain and not rain_df.empty:
                    rain_bars = alt.Chart(chart_data).mark_bar(opacity=0.12, size=4).encode(
                        x=alt.X('Date:T'),
                        y=alt.Y('Rainfall (mm):Q', axis=alt.Axis(title='Rain (mm)', titleColor='lightblue')),
                        color=alt.value('lightblue')
                    ).transform_filter(alt.datum.Type == 'Rainfall')
                    chart = alt.layer(chart, rain_bars).resolve_scale(y='independent')

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