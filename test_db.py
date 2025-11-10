import sqlite3

DB_FILE = '/home/river_levels_app/river_levels.db'

try:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Test insert (dummy)
    cursor.execute("INSERT INTO readings (station_id, river, label, level, timestamp) VALUES ('test', 'test', 'test', 1.0, '2025-11-09T00:00:00Z')")
    conn.commit()
    print("Insert successful")
    # Test query
    cursor.execute("SELECT * FROM readings WHERE station_id = 'test'")
    print(cursor.fetchone())
    # Clean up
    cursor.execute("DELETE FROM readings WHERE station_id = 'test'")
    conn.commit()
    print("Cleanup successful")
    conn.close()
except Exception as e:
    print(f"DB error: {e}")