import pandas as pd
from sqlalchemy import create_engine, Integer, Text

# Define the URL and file paths
data_url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv"
database_path = "cars.sqlite"

# Read the CSV file into a pandas DataFrame, skipping metadata rows and specifying column names
column_names = {
    0: "date",
    1: "CIN",
    2: "name",
    12: "petrol",
    22: "diesel",
    32: "gas",
    42: "electro",
    52: "hybrid",
    62: "plugInHybrid",
    72: "others"
}
df = pd.read_csv(data_url, encoding="latin1", sep=";", skiprows=6, skipfooter=4, engine="python", header=None)
df.rename(columns=column_names, inplace=True)

# Drop the first row
df.drop(index=0, inplace=True)

# Drop unwanted columns
columns_to_drop = [col for col in df.columns if col not in column_names.values()]
df.drop(columns=columns_to_drop, inplace=True)

# Convert columns to appropriate data types
df["CIN"] = df["CIN"].astype(float).astype(int).astype(str).str.zfill(5)

# Convert numeric columns to appropriate data types and fill null values with 0
numeric_columns = ["petrol", "diesel", "gas", "electro", "hybrid", "plugInHybrid", "others"]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

# Drop rows with null values in numeric columns
df = df.dropna(subset=numeric_columns)

# Define SQLite types for each column
sqlite_types = {
    "date": Text(),
    "CIN": Text(),
    "name": Text(),
    "petrol": Integer(),
    "diesel": Integer(),
    "gas": Integer(),
    "electro": Integer(),
    "hybrid": Integer(),
    "plugInHybrid": Integer(),
    "others": Integer()
}

# Create a connection to the SQLite database using SQLAlchemy
engine = create_engine(f"sqlite:///{database_path}")

# Write the DataFrame into a SQLite table called "cars" with appropriate SQLite types
df.to_sql("cars", engine, if_exists="replace", index=False, dtype=sqlite_types)

# Close the database connection
engine.dispose()
