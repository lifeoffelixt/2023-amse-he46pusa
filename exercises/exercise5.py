import urllib.request
import zipfile
import csv
import pandas as pd
from sqlalchemy import create_engine, Text, Float, Integer

# Download the GTFS data
url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
urllib.request.urlretrieve(url, 'GTFS.zip')

# Extract the ZIP file
with zipfile.ZipFile('GTFS.zip', 'r') as zip_ref:
    zip_ref.extractall('GTFS')

# Filter and validate the data
filtered_data = []
with open('GTFS/stops.txt', 'r', encoding='utf-8-sig') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    
    for row in csv_reader:
        if (
            row['zone_id'] == '2001'
            and row['stop_name'].isascii()
            and -90 <= float(row['stop_lat']) <= 90
            and -90 <= float(row['stop_lon']) <= 90
        ):
            filtered_data.append({
                'stop_id': row['stop_id'],
                'stop_name': row['stop_name'],
                'stop_lat': float(row['stop_lat']),
                'stop_lon': float(row['stop_lon']),
                'zone_id': int(row['zone_id'])
            })
        

# Convert filtered_data to a DataFrame
df = pd.DataFrame(filtered_data)

# Define SQLite types for each column
sqlite_types = {
    'stop_id': Integer(),
    'stop_name': Text(),
    'stop_lat': Float(),
    'stop_lon': Float(),
    'zone_id': Integer()
}

engine = create_engine('sqlite:///gtfs.sqlite')

df.to_sql('stops', engine, if_exists='replace', index=False, dtype=sqlite_types)

# Close the database connection
engine.dispose()
