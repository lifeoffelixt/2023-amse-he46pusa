import pytest
import sqlite3
import numpy as np
import os

import project.ETLPipeline as etl

@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown_session():
    # Function to run at the beginning of the test session
    print("Running setup at the start of the test session")
    # Run your function here
    etl.main(testing=True)

    # Teardown step
    yield

    # Code that runs at the end of the test session
    print("Running teardown at the end of the test session")
    remove_test_file()

def remove_test_file():
    # Remove the test database file
    if os.path.exists('project/test/test_data.sqlite'):
        os.remove('project/test/test_data.sqlite')

def test_tables_exist():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite') 

    # Get the list of table names in the database
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    print(table_names)
    # Check if the required tables exist
    required_tables = [
        'weatherData',
        'weatherDataID',
        'crashData2017',
        'crashData2018',
        'crashData2019',
        'crashDataNearby2017',
        'crashDataNearby2018',
        'crashDataNearby2019',
        'crashData',
        'weatherCrashData',
        'weatherCrashDataNormalized'
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
    distance = etl.calculate_distance(coords1, coords2)

    # Check if the distance is approximately 1 km (within a small tolerance)
    assert pytest.approx(distance, abs=10) == 1000, "Distance calculation incorrect."


def test_weatherdata_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the WeatherData table
    cursor.execute("PRAGMA table_info(weatherData);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("Strecke", "TEXT"),
        ("Lat [°]", "FLOAT"),
        ("Lon [°]", "FLOAT"),
        ("Nebel", "FLOAT"),
        ("Black Ice", "FLOAT"),
        ("Neuschnee", "FLOAT"),
        ("Gesamtschnee", "FLOAT"),
        ("Niederschlag", "FLOAT"),
        ("Wind", "FLOAT"),
        ("Windböen", "FLOAT"),
        ("Gesamt", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()

def test_crashData2017_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashData2017 table
    cursor.execute("PRAGMA table_info(crashData2017);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("OBJECTID", "BIGINT"),
        ("UIDENTSTLA", "TEXT"),
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("IstRad", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstFuss", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstSonstig", "BIGINT"),
        ("LICHT", "BIGINT"),
        ("STRZUSTAND", "BIGINT"),
        ("LINREFX", "FLOAT"),
        ("LINREFY", "FLOAT"),
        ("XGCSWGS84", "FLOAT"),
        ("YGCSWGS84", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()

def test_crashData2018_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashData2018 table
    cursor.execute("PRAGMA table_info(crashData2018);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("OBJECTID_1", "BIGINT"),
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("ULICHTVERH", "BIGINT"),
        ("IstRad", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstFuss", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstGkfz", "BIGINT"),
        ("IstSonstig", "BIGINT"),
        ("STRZUSTAND", "BIGINT"),
        ("LINREFX", "FLOAT"),
        ("LINREFY", "FLOAT"),
        ("XGCSWGS84", "FLOAT"),
        ("YGCSWGS84", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()

def test_crashData2019_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashData2019 table
    cursor.execute("PRAGMA table_info(crashData2019);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("OBJECTID", "BIGINT"),
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("ULICHTVERH", "BIGINT"),
        ("IstRad", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstFuss", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstGkfz", "BIGINT"),
        ("IstSonstige", "BIGINT"),
        ("LINREFX", "FLOAT"),
        ("LINREFY", "FLOAT"),
        ("XGCSWGS84", "FLOAT"),
        ("YGCSWGS84", "FLOAT"),
        ("STRZUSTAND", "BIGINT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()

def test_weatherDataID_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the weatherDataID table
    cursor.execute("PRAGMA table_info(weatherDataID);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("Strecke", "TEXT"),
        ("StreckeID", "TEXT"),
        ("Kilometer", "BIGINT"),
        ("Latitude", "FLOAT"),
        ("Longitude", "FLOAT"),
        ("Nebel", "FLOAT"),
        ("Black Ice", "FLOAT"),
        ("Neuschnee", "FLOAT"),
        ("Gesamtschnee", "FLOAT"),
        ("Niederschlag", "FLOAT"),
        ("Wind", "FLOAT"),
        ("Windböen", "FLOAT"),
        ("Gesamt", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()
    
def test_crashDataNearby2017_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashDataNearby2017 table
    cursor.execute("PRAGMA table_info(crashDataNearby2017);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstSonstig", "BIGINT"),
        ("LICHT", "BIGINT"),
        ("STRZUSTAND", "BIGINT"),
        ("Longitude", "FLOAT"),
        ("Latitude", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()
    
def test_crashDataNearby2018_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashDataNearby2018 table
    cursor.execute("PRAGMA table_info(crashDataNearby2018);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("ULICHTVERH", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstGkfz", "BIGINT"),
        ("IstSonstig", "BIGINT"),
        ("STRZUSTAND", "BIGINT"),
        ("Longitude", "FLOAT"),
        ("Latitude", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()
    
def test_crashDataNearby2019_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashDataNearby2019 table
    cursor.execute("PRAGMA table_info(crashDataNearby2019);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("ULICHTVERH", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstGkfz", "BIGINT"),
        ("IstSonstig", "BIGINT"),
        ("Longitude", "FLOAT"),
        ("Latitude", "FLOAT"),
        ("STRZUSTAND", "BIGINT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()
    
def test_crashData_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashData table
    cursor.execute("PRAGMA table_info(crashData);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("ULAND", "BIGINT"),
        ("UREGBEZ", "BIGINT"),
        ("UKREIS", "BIGINT"),
        ("UGEMEINDE", "BIGINT"),
        ("UJAHR", "BIGINT"),
        ("UMONAT", "BIGINT"),
        ("USTUNDE", "BIGINT"),
        ("UWOCHENTAG", "BIGINT"),
        ("UKATEGORIE", "BIGINT"),
        ("UART", "BIGINT"),
        ("UTYP1", "BIGINT"),
        ("IstPKW", "BIGINT"),
        ("IstKrad", "BIGINT"),
        ("IstSonstig", "BIGINT"),
        ("LICHT", "FLOAT"),
        ("STRZUSTAND", "BIGINT"),
        ("Longitude", "FLOAT"),
        ("Latitude", "FLOAT"),
        ("ULICHTVERH", "FLOAT"),
        ("IstGkfz", "FLOAT"),
        ("Strecke", "TEXT"),
        ("StreckeID", "TEXT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()
    
def test_weatherCrashData_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the weatherCrashDta table
    cursor.execute("PRAGMA table_info(weatherCrashData);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("Strecke", "TEXT"),
        ("StreckeID", "TEXT"),
        ("Kilometer", "BIGINT"),
        ("Latitude", "FLOAT"),
        ("Longitude", "FLOAT"),
        ("Nebel", "FLOAT"),
        ("Black Ice", "FLOAT"),
        ("Neuschnee", "FLOAT"),
        ("Gesamtschnee", "FLOAT"),
        ("Niederschlag", "FLOAT"),
        ("Wind", "FLOAT"),
        ("Windböen", "FLOAT"),
        ("Gesamt", "FLOAT"),
        ("CrashCount", "FLOAT"),
        ("NormalizedCrash", "FLOAT")
    ]

    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()

def test_weatherCrashDataNormalized_columns():
    # Connect to the SQLite database
    conn = sqlite3.connect('project/test/test_data.sqlite')  

    # Get the cursor
    cursor = conn.cursor()

    # Execute a query to get the columns of the crashDataNearby2019 table
    cursor.execute("PRAGMA table_info(weatherCrashDataNormalized);")
    columns = cursor.fetchall()

    # Define the expected column names and their corresponding data types
    expected_columns = [
        ("Strecke", "TEXT"),
        ("StreckeID", "TEXT"),
        ("Kilometer", "BIGINT"),
        ("Latitude", "FLOAT"),
        ("Longitude", "FLOAT"),
        ("Nebel", "FLOAT"),
        ("Black Ice", "FLOAT"),
        ("Neuschnee", "FLOAT"),
        ("Gesamtschnee", "FLOAT"),
        ("Niederschlag", "FLOAT"),
        ("Wind", "FLOAT"),
        ("Windböen", "FLOAT"),
        ("Gesamt", "FLOAT"),
        ("CrashCount", "FLOAT"),
        ("NormalizedCrash", "FLOAT")
    ]


    # Check if the columns and their data types match the expected ones
    for i, expected_column in enumerate(expected_columns):
        assert columns[i][1] == expected_column[0], f"Column name mismatch for column {i+1}"
        assert columns[i][2] == expected_column[1], f"Data type mismatch for column {i+1}"

    # Close the database connection
    conn.close()


def test_final_database_exists():
    assert os.path.exists('project/test/test_data.sqlite'), "The final database file does not exist."