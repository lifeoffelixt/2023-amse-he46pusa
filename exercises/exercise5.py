import urllib.request
import zipfile
import csv
from sqlalchemy import create_engine, Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Download the GTFS data
url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
urllib.request.urlretrieve(url, 'GTFS.zip')

# Extract the ZIP file
with zipfile.ZipFile('GTFS.zip', 'r') as zip_ref:
    zip_ref.extractall('GTFS')

# Define the desired columns and their data types
desired_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
desired_data_types = [Integer, String, Float, Float, Integer]

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
            filtered_data.append(row)

# Create the SQLite database and table using SQLAlchemy
Base = declarative_base()


class Stop(Base):
    __tablename__ = 'stops'
    stop_id = Column(Integer)
    stop_name = Column(String)
    stop_lat = Column(Float)
    stop_lon = Column(Float)
    zone_id = Column(Integer)


# Create the database engine and session
engine = create_engine('sqlite:///gtfs.sqlite')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Insert the filtered data into the stops table
for row in filtered_data:
    stop = Stop(
        stop_id=row['stop_id'],
        stop_name=row['stop_name'],
        stop_lat=float(row['stop_lat']),
        stop_lon=float(row['stop_lon']),
        zone_id=int(row['zone_id'])
    )
    session.add(stop)

# Commit the changes and close the session
session.commit()
session.close()

print("Data pipeline completed successfully!")
