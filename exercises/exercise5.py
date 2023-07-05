import urllib.request
import zipfile
import csv
import sqlite3

# Download the GTFS data
url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
urllib.request.urlretrieve(url, 'GTFS.zip')

# Extract the ZIP file
with zipfile.ZipFile('GTFS.zip', 'r') as zip_ref:
    zip_ref.extractall('GTFS')

# Define the desired columns and their data types
desired_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
desired_data_types = ['TEXT', 'TEXT', 'FLOAT', 'FLOAT', 'BIGINT']

# Filter and validate the data
filtered_data = []
with open('GTFS/stops.txt', 'r', encoding='utf-8-sig') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        if row['zone_id'] == '2001' and row['stop_name'].isascii() and \
           -90 <= float(row['stop_lat']) <= 90 and -90 <= float(row['stop_lon']) <= 90:
            filtered_data.append(row)

# Create the SQLite database and insert the data
connection = sqlite3.connect('gtfs.sqlite')
cursor = connection.cursor()

# Create the stops table with desired column names and types
columns_str = ', '.join(f'{col} {data_type}' for col, data_type in zip(desired_columns, desired_data_types))
create_table_query = f'CREATE TABLE stops ({columns_str})'
cursor.execute(create_table_query)

# Insert the filtered data into the stops table
for row in filtered_data:
    values_str = ', '.join('?' * len(desired_columns))
    insert_query = f'INSERT INTO stops ({", ".join(desired_columns)}) VALUES ({values_str})'
    cursor.execute(insert_query, tuple(row[col] for col in desired_columns))

# Commit the changes and close the connection
connection.commit()
connection.close()

print("Data pipeline completed successfully!")
