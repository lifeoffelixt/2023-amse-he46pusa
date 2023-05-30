import pytest
import sqlite3
import subprocess
import sys
import numpy as np

sys.path.append('../data')
from dataDownloadFilterPipeline import calculate_distance

def test_tables_exist():
    # Execute the dataDownloadFilterPipeline.py script to create the SQLite file
    subprocess.run(['python', '../data/dataDownloadFilterPipeline.py'])

    # Connect to the SQLite database
    conn = sqlite3.connect('../data/data.sqlite') 

    # Get the list of table names in the database
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    print(table_names)
    # Check if the required tables exist
    required_tables = [
        'WeatherData',
        'CrashData2017',
        'CrashData2018',
        'CrashData2019',
        'CrashData2017_nearby',
        'CrashData2018_nearby',
        'CrashData2019_nearby',
        'CrashData',
        'WeatherDataID',
        'WeatherCrashData'
    ]
    for table in required_tables:
        assert table in table_names, f"Table '{table}' does not exist."

    # Close the database connection
    conn.close()

def test_calculate_distance():
    # Define two dummy coordinates 1 km apart
    coords1 = np.array([(0, 0)])
    coords2 = np.array([(0.009, 0)])

    # Calculate the distance using the function
    distance = calculate_distance(coords1, coords2)

    # Check if the distance is approximately 1 km (within a small tolerance)
    assert pytest.approx(distance, abs=10) == 1000, "Distance calculation incorrect."

def test_weatherdata_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('../data/data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the WeatherData table
    cursor.execute("PRAGMA table_info(WeatherData);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("Strecke", "TEXT"),
        ("Latitude", "REAL"),
        ("Longitude", "REAL"),
        ("Nebel", "REAL"),
        ("Black Ice", "REAL"),
        ("Neuschnee", "REAL"),
        ("Gesamtschnee", "REAL"),
        ("Niederschlag", "REAL"),
        ("Wind", "REAL"),
        ("Windböen", "REAL"),
        ("Gesamt", "REAL")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()






