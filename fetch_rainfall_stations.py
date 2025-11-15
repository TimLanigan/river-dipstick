import requests
import ast  # For safely evaluating the STATIONS dict from file
import time  # For short delay if needed

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
    """Read river_reference.py, update STATIONS dict, and write back."""
    if not new_stations:
        print(f"No stations found for {new_river}—skipping update.")
        return

    # Read the full file content
    with open(REFERENCE_FILE, 'r') as f:
        content = f.readlines()

    # Find the line where STATIONS starts and collect the dict lines
    in_dict = False
    dict_lines = []
    new_content = []
    for line in content:
        if line.strip().startswith('STATIONS = {'):
            in_dict = True
            dict_lines.append(line.strip()[len('STATIONS = '):])  # Strip assignment
        if in_dict:
            dict_lines.append(line.strip())
            if line.strip().endswith('}'):
                in_dict = False
        else:
            new_content.append(line)

    # Reassemble and eval the dict string
    dict_str = ' '.join(dict_lines).strip('{}')  # Clean
    try:
        stations_dict = ast.literal_eval('{' + dict_str + '}')
    except SyntaxError as e:
        print(f"Error parsing STATIONS dict: {e}")
        print("Check river_reference.py for unbalanced braces or invalid syntax.")
        return

    # Add new river if not exists
    if new_river in stations_dict:
        print(f"{new_river} already exists—updating stations.")
    stations_dict[new_river] = new_stations

    # Generate new STATIONS assignment with proper formatting
    new_dict_str = "STATIONS = {\n"
    for r, s_list in stations_dict.items():
        new_dict_str += f"    '{r}': [\n"
        for s in s_list:
            new_dict_str += f"        {{'id': '{s['id']}', 'label': '{s['label']}', 'lat': {s['lat']}, 'lon': {s['lon']}}},\n"
        new_dict_str += "    ],\n"
    new_dict_str += "}\n"

    # Write back (append to non-dict content)
    with open(REFERENCE_FILE, 'w') as f:
        f.writelines(new_content[:new_content.index('# Add more rivers...') + 1])  # Up to comment
        f.write(new_dict_str)

    print(f"Updated river_reference.py with {new_river} and {len(new_stations)} stations.")

# Set the river to add (change this variable)
new_river = "River Lune"

# Fetch and update
new_stations = fetch_stations(new_river)
update_reference_file(new_river, new_stations)