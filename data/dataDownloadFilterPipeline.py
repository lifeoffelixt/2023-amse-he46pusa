import numpy as np
import pandas as pd
import subprocess


def dowloadData():
    # Path to jv script
    script_path = 'data/dataDownloadPipeline.jv'

    # Call the script using subprocess
    subprocess.call(['jv', script_path])

def filterData():
    weatherData_raw = pd.read_sql_table('WeatherData', 'sqlite:///data/data.sqlite')
    crashData2017_raw = pd.read_sql_table('CrashData2017', 'sqlite:///data/data.sqlite')
    crashData2018_raw = pd.read_sql_table('CrashData2018', 'sqlite:///data/data.sqlite')
    crashData2019_raw = pd.read_sql_table('CrashData2019', 'sqlite:///data/data.sqlite')
    weatherData = weatherData_raw.copy()

    crashData2017 = crashData2017_raw[crashData2017_raw['UMONAT'] == 12]
    crashData2019 = crashData2019_raw[crashData2019_raw['UMONAT'] != 12]
    crashData2018 = crashData2018_raw.copy()

    crashData2017 = crashData2017[crashData2017['IstRad'] != 1]
    crashData2018 = crashData2018[crashData2018['IstRad'] != 1]
    crashData2019 = crashData2019[crashData2019['IstRad'] != 1]
    crashData2017 = crashData2017[crashData2017['IstFuss'] != 1]
    crashData2018 = crashData2018[crashData2018['IstFuss'] != 1]
    crashData2019 = crashData2019[crashData2019['IstFuss'] != 1]

    crashData2017 = crashData2017[crashData2017['UTYP1'] != 3]
    crashData2018 = crashData2018[crashData2018['UTYP1'] != 3]
    crashData2019 = crashData2019[crashData2019['UTYP1'] != 3]
    crashData2017 = crashData2017[crashData2017['UART'] != 6]
    crashData2018 = crashData2018[crashData2018['UART'] != 6]
    crashData2019 = crashData2019[crashData2019['UART'] != 6]

    crashData2018['IstSonstig'] = crashData2018['IstSonstig'] | crashData2018['IstGkfz']
    crashData2019['IstSonstige'] = crashData2019['IstSonstige'] | crashData2019['IstGkfz']
    crashData2019 = crashData2019.rename(columns={"IstSonstige": "IstSonstig"})

    crashData2017 = crashData2017.drop(['LINREFX', 'LINREFY', 'IstRad', 'IstFuss', 'OBJECTID', 'UIDENTSTLA'], axis=1)
    crashData2018 = crashData2018.drop(['LINREFX', 'LINREFY', 'IstRad', 'IstFuss', 'OBJECTID_1'], axis=1)
    crashData2019 = crashData2019.drop(['LINREFX', 'LINREFY', 'IstRad', 'IstFuss', 'OBJECTID'], axis=1)

    # Get the column order from df1
    column_order = crashData2017.columns

    # Reorder the columns in df2 based on the column order from df1
    crashData2018 = crashData2018.reindex(columns=column_order)
    crashData2019 = crashData2019.reindex(columns=column_order)

    # Set a threshold distance for considering two locations as a match
    threshold_distance = 600  # 600 meters as the weather points are 1km apart and we want to match the closest one

    # Calculate distances using a loop
    distances = []
    for i, row in crashData2017.iterrows():
        lat1, lon1 = row['Latitude'], row['Longitude']
        dist = calculate_distance(np.array([(lat1, lon1)]), weatherData[['Latitude', 'Longitude']].to_numpy())
        distances.append(dist)

    # Find matching rows
    distances = np.concatenate(distances, axis=0)
    distances = distances.reshape(len(crashData2017), len(weatherData))
    crashData2017_nearby = crashData2017[np.min(distances, axis=1) <= threshold_distance]

    # Calculate distances using a loop
    distances = []
    for i, row in crashData2018.iterrows():
        lat1, lon1 = row['Latitude'], row['Longitude']
        dist = calculate_distance(np.array([(lat1, lon1)]), weatherData[['Latitude', 'Longitude']].to_numpy())
        distances.append(dist)

    # Find matching rows
    distances = np.concatenate(distances, axis=0)
    distances = distances.reshape(len(crashData2018), len(weatherData))
    crashData2018_nearby = crashData2018[np.min(distances, axis=1) <= threshold_distance]

    # Calculate distances using a loop
    distances = []
    for i, row in crashData2019.iterrows():
        lat1, lon1 = row['Latitude'], row['Longitude']
        dist = calculate_distance(np.array([(lat1, lon1)]), weatherData[['Latitude', 'Longitude']].to_numpy())
        distances.append(dist)

    # Find matching rows
    distances = np.concatenate(distances, axis=0)
    distances = distances.reshape(len(crashData2019), len(weatherData))
    crashData2019_nearby = crashData2019[np.min(distances, axis=1) <= threshold_distance]

    crashData2017_nearby.to_sql('CrashData2017_nearby', 'sqlite:///data/data.sqlite', if_exists='replace', index=False)
    crashData2018_nearby.to_sql('CrashData2018_nearby', 'sqlite:///data/data.sqlite', if_exists='replace', index=False)
    crashData2019_nearby.to_sql('CrashData2019_nearby', 'sqlite:///data/data.sqlite', if_exists='replace', index=False)

    crashData = pd.concat([crashData2017_nearby, crashData2018_nearby, crashData2019_nearby], ignore_index=True)

    # Generate artificial ID within each 'Strecke' group
    weatherData['UniqueID'] = weatherData.groupby('Strecke').cumcount() + 1

    # Create new column with values combining 'Strecke' and 'UniqueID'
    weatherData['StreckeID'] = weatherData['Strecke'] + '_' + weatherData['UniqueID'].astype(str)

    # Move 'StreckeID' column to the second position
    weatherData.insert(1, 'StreckeID', weatherData.pop('StreckeID'))

    # Drop the 'UniqueID' column
    weatherData.drop('UniqueID', axis=1, inplace=True)

    # Get the coordinates from both dataframes
    crash_coords = crashData[['Latitude', 'Longitude']].values
    weather_coords = weatherData[['Latitude', 'Longitude']].values

    # Create an empty list to store the corresponding Strecke values
    strecke_values = []
    streckeID_values = []

    # Iterate over each row in crashData
    for i, crash_row in crashData.iterrows():
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

    crashData.to_sql('CrashData', 'sqlite:///data/data.sqlite', if_exists='replace', index=False)

    weatherData.to_sql('WeatherDataID', 'sqlite:///data/data.sqlite', if_exists='replace', index=False)

def connectData():
    crashData = pd.read_sql_table('CrashData', 'sqlite:///data/data.sqlite')
    weatherData = pd.read_sql_table('WeatherDataID', 'sqlite:///data/data.sqlite')
    crashDataGrouped = crashData.groupby(['Strecke', 'StreckeID']).size().reset_index(name='CrashCount')
    # Perform a left join on "Strecke" and "StreckeID"
    weatherData = weatherData.merge(crashDataGrouped, on=['Strecke', 'StreckeID'], how='left')

    # Fill missing values with 0
    weatherData['CrashCount'] = weatherData['CrashCount'].fillna(0)
    # Calculate the minimum and maximum values of "Count"
    min_count = weatherData['CrashCount'].min()
    max_count = weatherData['CrashCount'].max()

    # Normalize the "Count" values to a range of 0 to 100
    weatherData['NormalizedCrashCount'] = (weatherData['CrashCount'] - min_count) / (max_count - min_count) * 100

    # Round the normalized values to at most 1 decimal point
    weatherData['NormalizedCrashCount'] = weatherData['NormalizedCrashCount'].round(decimals=1)

    weatherData.to_sql('WeatherCrashData', 'sqlite:///data/data.sqlite', if_exists='replace', index=False)


def calculate_distance(coords1, coords2):
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

def main():
    dowloadData()
    filterData()
    connectData()

if __name__ == "__main__":
    main()