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
    33: "gas",
    42: "electro",
    52: "hybrid",
    61: "plugInHybrid",
    69: "others"
}
df = pd.read_csv(data_url, encoding="latin1", sep=";", skiprows=6, skipfooter=4, engine="python", header=None)
df.rename(columns=column_names, inplace=True)

# Drop unwanted columns
columns_to_drop = [col for col in df.columns if col not in column_names.values()]
df.drop(columns=columns_to_drop, inplace=True)

# Convert columns to appropriate data types
df["CIN"] = df["CIN"].astype(str)
df["petrol"] = pd.to_numeric(df["petrol"], errors="coerce")
df["diesel"] = pd.to_numeric(df["diesel"], errors="coerce")
df["gas"] = pd.to_numeric(df["gas"], errors="coerce")
df["electro"] = pd.to_numeric(df["electro"], errors="coerce")
df["hybrid"] = pd.to_numeric(df["hybrid"], errors="coerce")
df["plugInHybrid"] = pd.to_numeric(df["plugInHybrid"], errors="coerce")
df["others"] = pd.to_numeric(df["others"], errors="coerce")

# Validate and clean the data
df = df[pd.to_numeric(df["CIN"], errors="coerce").notnull()]  # Filter valid CINs
df = df.astype({"CIN": str})  # Convert CIN column to string

# Drop rows with negative values and replace null values with 0
df = df[df.select_dtypes(include=["float64", "int64"]).ge(0).all(axis=1)]
df.fillna(0, inplace=True)

# Create a connection to the SQLite database using SQLAlchemy
engine = create_engine(f"sqlite:///{database_path}")

# Write the DataFrame into a SQLite table called "cars"
df.to_sql("cars", engine, if_exists="replace", index=False)

# Close the database connection
engine.dispose()
