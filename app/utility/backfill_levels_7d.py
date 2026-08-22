#!/usr/bin/env python3
"""One-off: last 7 days of levels so a fresh DB can draw charts. Same insert path as get_readings."""
import sys
import time
from datetime import datetime, timedelta, UTC

sys.path.append("/app")
from river_reference import STATIONS
from get_readings import CONNECTION_STRING, api_get
import psycopg2

DAYS = 7
SEPA_STATIONS = {"133148", "133170", "133176", "506155"}
SINCE = (datetime.now(UTC) - timedelta(days=DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")


def sepa_history(station_id):
    url = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"
    params = {
        "service": "kisters",
        "type": "queryServices",
        "datasource": "0",
        "request": "getTimeseriesValues",
        "ts_path": f"1/{station_id}/SG/15m.Cmd",
        "period": "P7D",
        "format": "json",
    }
    data = api_get(url, params)
    out = []
    if not data or not isinstance(data, list):
        return out
    for item in data:
        if isinstance(item, dict) and item.get("data"):
            for row in item["data"]:
                if len(row) >= 2 and row[1] is not None:
                    try:
                        out.append((float(row[1]), row[0]))
                    except (TypeError, ValueError):
                        pass
    return out


def ea_history(station_id):
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    data = api_get(url, {"parameter": "level", "since": SINCE, "_sorted": "", "_limit": 2000})
    out = []
    if not data or "items" not in data:
        return out
    for item in data["items"]:
        if item.get("value") is None:
            continue
        out.append((item["value"], item.get("dateTime")))
    return out


if __name__ == "__main__":
    print(f"=== Level backfill since {SINCE} ===")
    conn = psycopg2.connect(CONNECTION_STRING)
    cur = conn.cursor()
    total = 0
    for river, stations in STATIONS.items():
        for station in stations:
            sid = station["id"]
            label = station["label"]
            rows = sepa_history(sid) if sid in SEPA_STATIONS else ea_history(sid)
            n = 0
            for level, ts in rows:
                if not ts:
                    continue
                cur.execute(
                    """
                    INSERT INTO readings (station_id, river, label, level, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, timestamp) DO NOTHING
                    """,
                    (sid, river, label, level, ts),
                )
                n += cur.rowcount
            conn.commit()
            print(f"  {label} ({sid}): {n} new / {len(rows)} fetched")
            total += n
            time.sleep(0.4)
    cur.close()
    conn.close()
    print(f"=== done, {total} new rows ===")
