import requests
import pprint  # For pretty-printing the dict
from types import ModuleType  # For dynamic module
import sys

# API endpoint for stations
API_BASE = "https://environment.data.gov.uk/flood-monitoring/id/stations"

# Path to river_reference.py
REFERENCE_FILE = "river_reference.py"

def fetch_stations(river_name):
    """Fetch stations for a river name from EA API, with lat/long."""
    url = f"{API_BASE}?riverName={river_name.replace(' ', '%20')}&parameter=level"  # Filter for level stations
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()['items']
        stations = []
        for item in data:
            station_id = item['notation']
            label = item.get('label', 'Unknown')
            lat = item.get('lat', None)
            lon = item.get('long', None)
            stations.append({'id': station_id, 'label': label, 'lat': lat, 'lon': lon})
        return stations
    except Exception as e:
        print(f"Error fetching stations for {river_name}: {e}")
        return []

def update_reference_file(new_river, new_stations):
    """Load river_reference.py as module, update STATIONS, and write back."""
    if not new_stations:
        print(f"No stations found for {new_river}—skipping update.")
        return

    # Load the file as a module
    module = ModuleType("river_reference")
    with open(REFERENCE_FILE, 'r') as f:
        code = f.read()
    exec(code, module.__dict__)
    stations_dict = module.STATIONS

    # Add new river if not exists
    if new_river in stations_dict:
        print(f"{new_river} already exists—updating stations.")
    stations_dict[new_river] = new_stations

    # Generate new file content (keep comments, replace dict)
    with open(REFERENCE_FILE, 'r') as f:
        content = f.read()
    start = content.find('STATIONS =')
    end = content.rfind('}') + 1
    new_dict_str = 'STATIONS = ' + pprint.pformat(stations_dict, width=80, compact=False) + '\n'
    new_content = content[:start] + new_dict_str + content[end:]

    with open(REFERENCE_FILE, 'w') as f:
        f.write(new_content)
    print(f"Updated river_reference.py with {new_river} and {len(new_stations)} stations.")

# Set the river to add (change this variable)
new_river = "River Lune"

# Fetch and update
new_stations = fetch_stations(new_river)
update_reference_file(new_river, new_stations)