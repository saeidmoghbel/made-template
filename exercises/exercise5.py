import urllib.request
import zipfile
import os
import csv
import sqlite3

# Download the GTFS ZIP file using urllib.request.urlretrieve
url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
zip_file_path, _ = urllib.request.urlretrieve(url, filename="gtfs_data.zip")

# Extract stops.txt from the downloaded ZIP file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extract("stops.txt", path=".")

# Define function to validate data
def validate_data(stop_id, stop_name, stop_lat, stop_lon, zone_id):
    # Check if stop_id is numeric
    if not stop_id.isdigit():
        return False
    # Check if stop_name contains German umlauts
    if any(char in stop_name for char in ['ä', 'ö', 'ü', 'ß']):
        # Check if stop_lat and stop_lon are within valid range
        if -90 <= float(stop_lat) <= 90 and -90 <= float(stop_lon) <= 90:
            return True
    return False

# Create SQLite database connection
conn = sqlite3.connect('gtfs.sqlite')
cursor = conn.cursor()

# Create "stops" table in SQLite database
cursor.execute('''CREATE TABLE stops
                  (stop_id INTEGER PRIMARY KEY,
                   stop_name TEXT,
                   stop_lat FLOAT,
                   stop_lon FLOAT,
                   zone_id BIGINT)''')

# Read stops.txt, filter data, validate and insert into SQLite database
with open("stops.txt", newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row['zone_id'] == '2001':
            if validate_data(row['stop_id'], row['stop_name'], row['stop_lat'], row['stop_lon'], row['zone_id']):
                cursor.execute('''INSERT INTO stops (stop_id, stop_name, stop_lat, stop_lon, zone_id)
                                  VALUES (?, ?, ?, ?, ?)''',
                               (int(row['stop_id']), row['stop_name'], float(row['stop_lat']), float(row['stop_lon']), int(row['zone_id'])))

# Commit changes and close database connection
conn.commit()
conn.close()

# Remove downloaded ZIP file
os.remove(zip_file_path)

print("ETL pipeline completed successfully.")
