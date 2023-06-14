import pandas as pd
from sqlalchemy import create_engine

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

# Drop unwanted columns
columns_to_drop = [col for col in df.columns if col not in column_names.values()]
df.drop(columns=columns_to_drop, inplace=True)

# Convert columns to appropriate data types
df["CIN"] = df["CIN"].astype(str)

# Validate and clean the data
df = df[pd.to_numeric(df["CIN"], errors="coerce").notnull()]  # Filter valid CINs
df = df.astype({"CIN": str})  # Convert CIN column to string

# Convert numeric columns to appropriate data types and fill null values with 0
numeric_columns = ["petrol", "diesel", "gas", "electro", "hybrid", "plugInHybrid", "others"]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

# Drop rows with negative values
df = df[df[numeric_columns].ge(0).all(axis=1)]

# Create a connection to the SQLite database using SQLAlchemy
engine = create_engine(f"sqlite:///{database_path}")

# Write the DataFrame into a SQLite table called "cars"
df.to_sql("cars", engine, if_exists="replace", index=False)

# Close the database connection
engine.dispose()
