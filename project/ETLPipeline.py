import pandas as pd
import numpy as np
import os
import requests
import zipfile
import shutil
import sqlite3
from tqdm import tqdm

    
def extract(url: str, testing: bool = False) -> pd.DataFrame:
    # check if url is csv or zip
    if url.endswith('.csv'):
        df = pd.read_csv(url, sep=';')
    elif url.endswith('.zip'):
        df = handle_crash_zip(url)
    else:
        raise ValueError("URL must be a .csv or .zip file.")

    if testing:
        df = df.sample(frac=.05)

    return df


def transform(location: str = 'sqlite:///data/data.sqlite') -> None:
    if not table_exists("weatherDataID", location):
        load("weatherDataID", preprocess_weather_data(location), location)
    years = [2017, 2018, 2019]
    for _, year in tqdm(enumerate(years), total=len(years), desc="Preproccesing Years"):
        if not table_exists("crashDataNearby" + str(year), location):
            crashData = preprocess_crash_data("crashData" + str(year), year, location)
            crashData = connect_crash_data_with_weather_data("crashData" + str(year), crashData, 600, location)
            load("crashDataNearby" + str(year), crashData , location)

    if not table_exists("crashData", location):
        load("crashData", assign_crash_to_weather_data(600,location), location)

    if not table_exists("weatherCrashData", location):
        weatherCrashData = combine_weather_and_crash_data(location)
        weatherCrashData = add_column_with_normalized_crash_values(weatherCrashData)
        load("weatherCrashData", weatherCrashData, location)

    if not table_exists("weatherCrashDataNormalized", location):
        load("weatherCrashDataNormalized", normalize_per_Route(location), location)

        
def load(name: str, data: pd.DataFrame, location: str = 'sqlite:///data/data.sqlite') -> None:
    data.to_sql(name, location, if_exists='replace', index=False)


def handle_crash_zip(zip_url:str) -> pd.DataFrame:
    # Create the 'tmp' folder if it doesn't exist
    if not os.path.exists('tmp'):
        os.makedirs('tmp')

    # Download the zip file
    response = requests.get(zip_url)
    zip_file_path = os.path.join('tmp', 'data.zip')

    try:
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(response.content)

        # Extract the zip file
        extract_folder = os.path.join('tmp', 'extracted')
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)

        # Find the first CSV file in the 'extracted' folder
        csv_folder = os.path.join(extract_folder, 'csv')
        txt_files = [f for f in os.listdir(csv_folder) if f.endswith('.txt')]
        if len(txt_files) > 0:
            first_txt_file = os.path.join(csv_folder, txt_files[0])
        else:
            raise ValueError("No txt files found in the 'csv' folder.")

        # Load CSV file into a Pandas DataFrame
        df = pd.read_csv(first_txt_file, sep=';', low_memory=False, decimal=',')

        # Now you can work with the DataFrame as needed
        return df

    except Exception as e:
        print("Error:", e)

    finally:
        # Remove the 'tmp' folder
        shutil.rmtree('tmp')


def read_table_from_sqlite(name: str, location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    try:
        return pd.read_sql_table(name, location)
    except:
        raise ValueError("Table not found in database.")


def preprocess_weather_data(location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    weatherData = read_table_from_sqlite('weatherData', location)

    # Rename columns to make them easier to read
    weatherData = weatherData.rename(columns={'Lon [°]': 'Longitude', 'Lat [°]': 'Latitude'})
    # Generate artificial ID within each 'Strecke' group
    weatherData['IDperStrecke'] = weatherData.groupby('Strecke').cumcount() + 1

    # Create new column with values combining 'Strecke' and 'UniqueID'
    weatherData['StreckeID'] = weatherData['Strecke'] + '_' + weatherData['IDperStrecke'].astype(str)

    # Move 'StreckeID' column to the second position
    weatherData.insert(1, 'StreckeID', weatherData.pop('StreckeID'))

    # Move 'IDperStrecke' column to the third position
    weatherData.insert(2, 'IDperStrecke', weatherData.pop('IDperStrecke'))

    # return weatherData
    return weatherData


def preprocess_crash_data(name: str, year: int, location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    crashData = read_table_from_sqlite(name, location)
    # Rename columns to make them easier to read
    crashData = crashData.rename(columns={'XGCSWGS84': 'Longitude', 'YGCSWGS84': 'Latitude'})
    
    # Depending on the year the data has to be preprocessed differently
    if year == 2017:
        crashData = crashData[crashData['UMONAT'] == 12]
        crashData = crashData.drop(['LINREFX', 'LINREFY', 'OBJECTID', 'UIDENTSTLA'], axis=1)
    elif year == 2018:
        crashData['IstSonstig'] = crashData['IstSonstig'] | crashData['IstGkfz']
        crashData = crashData.drop(['LINREFX', 'LINREFY', 'OBJECTID_1'], axis=1)
    elif year == 2019:
        crashData = crashData[crashData['UMONAT'] != 12]
        crashData['IstSonstige'] = crashData['IstSonstige'] | crashData['IstGkfz']
        crashData = crashData.rename(columns={"IstSonstige": "IstSonstig"})
        crashData = crashData.drop(['LINREFX', 'LINREFY', 'OBJECTID'], axis=1)
    else:
        raise ValueError("Year must be 2017, 2018 or 2019.")
    
    # Drop rows with 'IstRad' or 'IstFuss' equal to 1
    crashData = crashData[crashData['IstRad'] != 1]
    crashData = crashData[crashData['IstFuss'] != 1]
    # Drop rows with 'UTYP1' equal to 3
    crashData = crashData[crashData['UTYP1'] != 3]
    # Drop rows with 'UART' equal to 6
    crashData = crashData[crashData['UART'] != 6]
    # Drop columns 'IstRad' and 'IstFuss'
    crashData = crashData.drop(['IstRad', 'IstFuss'], axis=1)
    # return crashData
    return crashData


def connect_crash_data_with_weather_data(name: str,
                                        crashData: pd.DataFrame,
                                        threshold_distance: int = 600,
                                        location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    
    weatherData = read_table_from_sqlite('weatherDataID', location)

    distances = []
    for _, row in tqdm(crashData.iterrows(), total=crashData.shape[0], desc=f"Connecting {name} with weatherData"):
        lat1, lon1 = row['Latitude'], row['Longitude']
        dist = calculate_distance(np.array([(lat1, lon1)]), weatherData[['Latitude', 'Longitude']].to_numpy())
        distances.append(dist)

    # Find matching rows
    distances = np.concatenate(distances, axis=0)
    distances = distances.reshape(len(crashData), len(weatherData))
    return crashData[np.min(distances, axis=1) <= threshold_distance]
    

def assign_crash_to_weather_data(threshold_distance: int = 600, location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    crashData = concat_crash_data(location)
    weatherData = read_table_from_sqlite('weatherDataID', location)

    crash_coords = crashData[['Latitude', 'Longitude']].values
    weather_coords = weatherData[['Latitude', 'Longitude']].values
    # Create an empty list to store the corresponding Strecke values
    strecke_values = []
    streckeID_values = []

    # Iterate over each row in crashData
    for i, _ in tqdm(crashData.iterrows(), total=crashData.shape[0], desc=f"Assigning Strecke from weatherData to crashData"):
        crash_coord = crash_coords[i]
        distances = calculate_distance(np.array([crash_coord]), weather_coords)
        closest_weather_idx = np.argmin(distances)
        closest_distance = distances[closest_weather_idx]

        # Assign the corresponding Strecke value if the closest distance is within the threshold
        if closest_distance <= threshold_distance:
            strecke_value = weatherData.loc[closest_weather_idx, 'Strecke']
            streckeID_value = weatherData.loc[closest_weather_idx, 'StreckeID']
        else:
            strecke_value = None
            streckeID_value = None

        strecke_values.append(strecke_value)
        streckeID_values.append(streckeID_value)

    # Assign the calculated Strecke values to a new column in crashData
    crashData['Strecke'] = strecke_values
    crashData['StreckeID'] = streckeID_values

    return crashData


def concat_crash_data(location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    try:
        crashDataNearby2017 = read_table_from_sqlite('crashDataNearby2017', location)
        crashDataNearby2018 = read_table_from_sqlite('crashDataNearby2018', location)
        crashDataNearby2019 = read_table_from_sqlite('crashDataNearby2019', location)
        return pd.concat([crashDataNearby2017, crashDataNearby2018, crashDataNearby2019], ignore_index=True)
    except:
        raise ValueError("Crash data from one year not found.")


def combine_weather_and_crash_data(location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    weatherData = read_table_from_sqlite('weatherDataID', location)
    crashData = read_table_from_sqlite("crashData", location)

    crashDataGrouped = crashData.groupby(['Strecke', 'StreckeID']).size().reset_index(name='CrashCount')
    # Perform a left join on "Strecke" and "StreckeID"
    combinedData = weatherData.merge(crashDataGrouped, on=['Strecke', 'StreckeID'], how='left')

    return combinedData


def add_column_with_normalized_crash_values(combinedData: pd.DataFrame) -> pd.DataFrame:
    # Fill missing values with 0
    combinedData['CrashCount'] = combinedData['CrashCount'].fillna(0)
    # Calculate the minimum and maximum values of "Count"
    min_count = combinedData['CrashCount'].min()
    max_count = combinedData['CrashCount'].max()

    # Normalize the "Count" values to a range of 0 to 100
    combinedData['NormalizedCrash'] = (combinedData['CrashCount'] - min_count) / (max_count - min_count) * 100

    # Round the normalized values to at most 1 decimal point
    combinedData['NormalizedCrash'] = combinedData['NormalizedCrash'].round(decimals=1)

    return combinedData


def normalize_per_Route(location: str = 'sqlite:///data/data.sqlite') -> pd.DataFrame:
    weatherCrashData = read_table_from_sqlite('weatherCrashData', location)
    # Define the columns to be normalized
    columns_to_normalize = ['Nebel', 'Black Ice', 'Neuschnee', 'Gesamtschnee', 'Niederschlag', 'Wind', 'Windböen', 'Gesamt', 'NormalizedCrash']

    # Create a new dataframe to store the normalized data
    WeatherCrashDataNormalized = weatherCrashData.copy()

    # Group the data by the 'Strecke' column
    grouped_data = WeatherCrashDataNormalized.groupby('Strecke')

    # Apply Min-Max scaling to each column within each group
    for col in columns_to_normalize:
        if WeatherCrashDataNormalized[col].dtype != object:
            min_val = grouped_data[col].transform('min')
            max_val = grouped_data[col].transform('max')
            WeatherCrashDataNormalized[col] = (WeatherCrashDataNormalized[col] - min_val) / (max_val - min_val)

    # The 'normalized_data' dataframe now contains the normalized values per route
    return WeatherCrashDataNormalized




def table_exists(table_name: str, location: str = 'data/data.sqlite') -> bool:
    # Extract the file path from the location
    path = location.replace('sqlite:///', '')
    # Connect to the SQLite database file
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Execute a query to check if the table exists
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")

    # Fetch the first result
    result = cursor.fetchone()

    # Close the database connection
    cursor.close()
    conn.close()

    # Return True if a result was found, indicating the table exists
    return result is not None


def calculate_distance(coords1: np.array, coords2: np.array) -> np.array:
    # Radius of the Earth in meters
    radius = 6371000

    # Convert latitudes and longitudes to radians
    lat1_rad = np.radians(coords1[:, 0])
    lon1_rad = np.radians(coords1[:, 1])
    lat2_rad = np.radians(coords2[:, 0])
    lon2_rad = np.radians(coords2[:, 1])

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = radius * c
    return distance


def print_message(message: str) -> None:
    print('------------------')
    print(message)
    print('------------------')


def main(testing: bool = False) -> None:
    print_message(f'Testing: {testing}')
    if testing:
        location = 'sqlite:///project/test/test_data.sqlite'
    else:
        location = 'sqlite:///data/data.sqlite'

    
    print_message('Begin Extracting')
    urls = ["https://www.mcloud.de/downloads/mcloud/96EA9CD1-0695-4461-90B1-BC6F6B0E0729/Resultat_HotSpot_Analyse_neu.csv",
            "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2017_EPSG25832_CSV.zip",
            "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2018_EPSG25832_CSV.zip",
            "https://www.opengeodata.nrw.de/produkte/transport_verkehr/unfallatlas/Unfallorte2019_EPSG25832_CSV.zip"]
    if not table_exists("weatherData", location):
        load("weatherData", extract(urls[0], testing), location)

    years = [2017, 2018, 2019]
    #for i, year in tqdm(enumerate(years), total=len(years), desc="Extracting Years"):
    for i, year in enumerate(years):
        if not table_exists("crashData"+str(year), location):
            load("crashData"+str(year), extract(urls[i+1], testing), location)
    print_message('Finished Extracting')
    
    print_message('Begin Transforming')
    transform(location)
    print_message('Finished Transforming')



if __name__ == "__main__":
    main(False)