import pandas as pd
from sqlalchemy import create_engine, Integer, Text, Float

# URL of the CSV file
url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(url, delimiter=";", error_bad_lines=False)

# Define SQLite types for columns
column_types = {
    "column_1": Integer(),
    "column_2": Text(),
    "column_3": Text(),
    "column_4": Text(),
    "column_5": Text(),
    "column_6": Text(),
    "column_7": Float(),
    "column_8": Float(),
    "column_9": Integer(),
    "column_10": Integer(),
    "column_11": Text(),
    "column_12": Text(),
    "geo_punkt": Text()
}

# SQLite database connection
engine = create_engine("sqlite:///airports.sqlite")

# Store the DataFrame in the SQLite database with assigned types
df.to_sql("airports", engine, if_exists="replace", index=False, dtype=column_types)

# Close the database connection
engine.dispose()
