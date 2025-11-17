#!/usr/bin/env python3
import requests
import csv

date_str = "2025-11-15"
url = f"https://environment.data.gov.uk/flood-monitoring/archive/readings-full-{date_str}.csv"

print(f"Fetching {url}...")
response = requests.get(url, stream=True)
if response.status_code != 200:
    print("Failed")
    exit()

rain_measures = set()
for line in response.iter_lines():
    if b"rainfall" in line.lower():
        try:
            reader = csv.DictReader([line.decode('utf-8')])
            for row in reader:
                measure = row.get('measure', '')
                if measure and 'rainfall' in measure:
                    rain_measures.add(measure)
        except:
            continue

print("\n=== RAINFALL MEASURE URLs FOUND ===")
for m in sorted(rain_measures):
    print(m)