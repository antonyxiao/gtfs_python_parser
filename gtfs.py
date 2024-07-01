import os
import time
import math
import sqlite3
from datetime import datetime
import urllib.request
import zipfile
import csv


class GTFS:
    STATIC_DIR = 'gtfs_static'
    SQLITE_DIR = 'sqlite3_db'
    
    def __init__(self, agency, city, url=None, update_db=True):
        self.agency = agency
        self.city = city
        self.db_path = f'{self.SQLITE_DIR}/{self.agency}_{self.city}.db'
        
        if not self.gtfs_exists():
            self.fetch_static_gtfs(url)
           
        if self.db_exists():
            if update_db:
                self.gtfs_to_sql()
        else:
            self.gtfs_to_sql()
            
    def db_exists(self):
        return os.path.isfile(f'{self.SQLITE_DIR}/{self.agency}_{self.city}.db')
            
    def gtfs_exists(self):
        '''
        returns whether the gtfs static file for the agency and city exists
        '''
        return os.path.isdir( f'{self.STATIC_DIR}/{self.agency}/{self.city}')
            
    def load_data(self, cursor, file_path, table_name, columns, primary_key_column=None):
        unique_keys = set() if primary_key_column else None
        
        with open(file_path, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            to_db = []
            for row in reader:
                if primary_key_column:
                    key = row[primary_key_column]
                    if key not in unique_keys:
                        unique_keys.add(key)
                        to_db.append(tuple(row[col] for col in columns))
                else:
                    to_db.append(tuple(row[col] for col in columns))
            
            placeholders = ', '.join(['?'] * len(columns))
            query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
            
            # Assuming 'cursor' is defined and connected to your database
            cursor.executemany(query, to_db)
    
    def gtfs_to_sql(self):
        '''
        1.	Create the SQLite database and tables.
        2.	Read data from text files.
        3.	Insert the data into the corresponding tables in the SQLite database.
        '''
        os.makedirs(GTFS.SQLITE_DIR, exist_ok=True)
        
        # Connect to SQLite database (or create it if it doesn't exist)
        # BC Transit static
        # - shape_id is not a primary key
        #
        # the following stop_ids are in stops but not stop_times
        # 930000 'Langford Transit Centre'
        # 900000 'Victoria Transit Centre'
        # 100000 'UVic Exch Bay D'
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.executescript("""
        
        DROP TABLE IF EXISTS agency;
        CREATE TABLE agency (
            agency_id TEXT NOT NULL PRIMARY KEY,
            agency_name TEXT,
            agency_url TEXT,
            agency_timezone TEXT,
            agency_phone TEXT,
            agency_lang TEXT
        );
        
        DROP TABLE IF EXISTS shapes;
        CREATE TABLE shapes (
            shape_id TEXT,
            shape_pt_lat REAL,
            shape_pt_lon REAL,
            shape_pt_sequence TEXT
        );
        
        DROP TABLE IF EXISTS calendar;
        CREATE TABLE calendar (
            service_id TEXT NOT NULL PRIMARY KEY,
            monday INTEGER,
            tuesday INTEGER,
            wednesday INTEGER,
            thursday INTEGER,
            friday INTEGER,
            saturday INTEGER,
            sunday INTEGER,
            start_date TEXT,
            end_date TEXT
        );
        
        DROP TABLE IF EXISTS calendar_dates;
        CREATE TABLE calendar_dates (
            service_id TEXT,
            date TEXT,
            exception_type INTEGER,
            FOREIGN KEY (service_id) REFERENCES calendar(service_id)
        );
        
        DROP TABLE IF EXISTS routes;
        CREATE TABLE routes (
            route_id TEXT NOT NULL PRIMARY KEY,
            agency_id TEXT,
            route_short_name TEXT,
            route_long_name TEXT,
            route_desc TEXT,
            route_type INTEGER,
            route_url TEXT,
            route_color TEXT,
            route_text_color TEXT,
            FOREIGN KEY (agency_id) REFERENCES agency(agency_id)
        );
        
        DROP TABLE IF EXISTS trips;
        CREATE TABLE trips (
            trip_id TEXT NOT NULL PRIMARY KEY,
            service_id TEXT,
            route_id TEXT,
            trip_headsign TEXT,
            direction_id INTEGER,
            shape_id TEXT,
            FOREIGN KEY (service_id) REFERENCES calendar(service_id),
            FOREIGN KEY (shape_id) REFERENCES shapes(shape_id)
        );
        
        DROP TABLE IF EXISTS stops;
        CREATE TABLE stops (
            stop_id TEXT NOT NULL PRIMARY KEY,
            stop_code TEXT,
            stop_name TEXT,
            stop_lat REAL,
            stop_lon REAL,
            location_type INTEGER,
            parent_station TEXT,
            wheelchair_boarding INTEGER,
            stop_desc TEXT,
            zone_id TEXT
        );
        
        DROP TABLE IF EXISTS stop_times;
        CREATE TABLE stop_times (
            trip_id TEXT,
            stop_id TEXT,
            stop_sequence TEXT,
            arrival_time TEXT,
            departure_time TEXT,
            stop_headsign TEXT,
            pickup_type INTEGER,
            drop_off_type INTEGER,
            shape_dist_traveled TEXT,
            timepoint INTEGER,
            FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
            FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
        );
        
        DROP TABLE IF EXISTS frequencies;
        CREATE TABLE frequencies (
            trip_id TEXT,
            start_time TEXT,
            end_time TEXT,
            headway_secs TEXT,
            FOREIGN KEY (trip_id) REFERENCES trips(trip_id)
        );
        """)
        
        ''' Uses the first column as the key in the dict
        # Function to load data from a CSV file into a table
        def load_data(file_path, table_name, columns):
            primary_key = columns[0]  # Assuming the first column is the primary key
            unique_keys = set()
            
            with open(file_path, 'r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                to_db = []
                for row in reader:
                    key = row[primary_key]
                    if key not in unique_keys:
                        unique_keys.add(key)
                        to_db.append(tuple(row[col] for col in columns))
                
                placeholders = ', '.join(['?'] * len(columns))
                query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
                cursor.executemany(query, to_db)
        '''
        
        # File paths (adjust these paths as necessary)
        files_to_load = {
            'agency.txt': ('agency', ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_phone', 'agency_lang']),
            'shapes.txt': ('shapes', ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence']),
            #'calendar.txt': ('calendar', ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date']),
            'calendar_dates.txt': ('calendar_dates', ['service_id', 'date', 'exception_type']),
            'routes.txt': ('routes', ['route_id', 'route_short_name', 'route_long_name', 'route_type', 'route_color', 'route_text_color']),
            'stops.txt': ('stops', ['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon', 'wheelchair_boarding']),
            'trips.txt': ('trips', ['trip_id', 'service_id', 'route_id', 'trip_headsign', 'direction_id', 'shape_id']),
            #'frequencies.txt': ('frequencies', ['trip_id', 'start_time', 'end_time', 'headway_secs']),
            'stop_times.txt': ('stop_times', ['trip_id', 'stop_id', 'stop_sequence', 'arrival_time', 'departure_time', 'stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint'])
        }
        
        directory = f'{self.STATIC_DIR}/{self.agency}/{self.city}/'
        
        # Load data from each file into the corresponding table
        for file_path, (table_name, columns) in files_to_load.items():
            self.load_data(cursor, directory + file_path, table_name, columns)
        
        # Commit the changes and close the connection
        conn.commit()
        conn.close()
        
        print("Data has been successfully loaded into the SQLite database.")
    
    def fetch_static_gtfs(self, url):
        '''
        Fetches static GTFS zip file from URL
        extracts the zip file into the GTFS directory 
        '''
        
        if not url:
            # if the list of transit file links does not exist
            if not os.path.isfile('transit_gtfs_list.csv'):
                print('No GTFS links provided to constructor and transit_gtfs_list.csv does not exist.')
                return  
                
            # load file urls from the list of transit links
            with open('transit_gtfs_list.csv', newline='') as transit_list:
                spamreader = csv.reader(transit_list, delimiter=',')
                for row in spamreader:
                    if row[0] == self.agency and row[1] == self.city:
                        # 3rd column is the link for the static files
                        url = row[2]
        
        # Path where you want to save the downloaded file
        save_path = f'{self.STATIC_DIR}/{self.agency}/{self.city}/{self.city}.zip'
        extract_path = f'{self.STATIC_DIR}/{self.agency}/{self.city}'
        
        # Ensure the extraction directory exists
        os.makedirs(extract_path, exist_ok=True)
        
        # Download the file from the URL
        urllib.request.urlretrieve(url, save_path)
        
        print(f"GTFS zip file successfully downloaded to {save_path}")
        
        # Extract the contents of the ZIP file
        with zipfile.ZipFile(save_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            print(f"Contents extracted to {extract_path}")
            
        os.remove(save_path)
        
        print(f"File {save_path} has been deleted.")        
        
    def get_date(self):
        return datetime.now().strftime('%Y%m%d')
        
    def get_time(self):
        return datetime.now().strftime('%H:%M:%S')
        

    def get_incoming_buses(self, stop_id, query_date=None, query_time=None, count = 1):
        '''
        parameters: db_path: path to sqlite GTFS db file
                    stop_id: stop sign id
                    row_count: how many rows to return
    
        return: List[(arrival_time, route_id, trip_headsign)]
        '''
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get the current time and date
        if not query_date:
            query_date = self.get_date()
        if not query_time:
            query_time = self.get_time()
            
        #current_weekday = now.strftime('%w')  # '0' is Sunday, '1' is Monday, ..., '6' is Saturday
        
        #print(f"Current time: {current_time}, Current date: {current_date}")
    
        # Define the query
        query = """
        SELECT
            st.arrival_time,
            t.route_id,
            t.trip_headsign,
            t.trip_id
        FROM
            stop_times st
        JOIN
            trips t ON st.trip_id = t.trip_id
        JOIN
            calendar_dates cd ON t.service_id = cd.service_id
        WHERE
            st.stop_id = ?
            AND st.arrival_time > ?
            AND cd.date = ?
        ORDER BY
            st.arrival_time
        LIMIT ?;
            
        """
        
        # Execute the query with the specified stop_id and current time/date
        cursor.execute(query, (stop_id, query_time, query_date, count))
            
        # Fetch all results
        incoming_buses = cursor.fetchall()
        
        # Close the database connection
        conn.close()
        
        return incoming_buses
        
    def haversine(self, lon1, lat1, lon2, lat2):
        # Convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
        
        # Haversine formula
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a)) 
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        return c * r
    
    """ uses non standard sqlite3 math functions 
    def get_nearby_bus_stops(self, lon, lat, radius_km=1, limit=0):
        # Convert radius from kilometers to meters
        radius_m = radius_km * 1000
    
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Haversine in SQL
        query = f'''
            SELECT 
                stop_id, 
                stop_name, 
                stop_lat, 
                stop_lon,
                (6371000 * acos(
                    cos(radians({lat})) * cos(radians(stop_lat)) * cos(radians(stop_lon) - radians({lon})) + 
                    sin(radians({lat})) * sin(radians(stop_lat))
                )) AS distance
            FROM stops
            WHERE distance <= {radius_m}
            ORDER BY distance
            LIMIT ?;
        '''
    
        cursor.execute(query, (limit,))
        nearby_stops = cursor.fetchall()
        
        # Close the database connection
        conn.close()
        
        return nearby_stops
    """
    def get_nearby_bus_stops(self, lon, lat, radius_km=1, limit=0):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Fetch all stops
        query = '''
            SELECT stop_id, stop_name, stop_lat, stop_lon
            FROM stops;
        '''
        
        cursor.execute(query)
        all_stops = cursor.fetchall()
        
        # Close the database connection
        conn.close()
        
        # Filter and sort stops by distance
        nearby_stops = []
        for stop in all_stops:
            stop_id, stop_name, stop_lat, stop_lon = stop
            distance = self.haversine(lon, lat, stop_lon, stop_lat)
            if distance <= radius_km:
                nearby_stops.append((stop_id, stop_name, stop_lat, stop_lon, distance))
        
        # Sort by distance
        nearby_stops.sort(key=lambda x: x[4])
        
        # Limit results if specified
        if limit > 0:
            nearby_stops = nearby_stops[:limit]
        
        return nearby_stops

    def get_all_trip_stops(self, route, direction, query_date=None, query_time=None, offset=0):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
        WITH today_trips AS (
            SELECT 
                trips.trip_id, 
                trips.route_id, 
                trips.service_id, 
                trips.direction_id
            FROM 
                trips
            INNER JOIN calendar_dates ON 
                trips.service_id = calendar_dates.service_id
            WHERE 
                trips.route_id = ?
            AND 
                trips.direction_id = ?
            AND
                calendar_dates.date = ?
        )
        """
        
        query += """
        ,trip_times AS (SELECT 
                st.trip_id,
                MIN(st.arrival_time) AS next_arrival_time
            FROM 
                stop_times st
            INNER JOIN today_trips tt ON 
                st.trip_id = tt.trip_id
            GROUP BY 
                st.trip_id
            ORDER BY 
                next_arrival_time
        )
        """
        
        query += """
        ,next_trip AS (
            SELECT 
                *
            FROM
                trip_times
            WHERE
                next_arrival_time > ?
            LIMIT ? OFFSET ?
        )
        """
        
        query += """
        SELECT 
            st.stop_id, 
            st.stop_sequence,
            stops.stop_name,
            st.arrival_time
        FROM 
            stop_times st
        INNER JOIN stops ON 
            st.stop_id = stops.stop_id
        INNER JOIN next_trip nt ON 
            st.trip_id = nt.trip_id
        ORDER BY
            CAST(st.stop_sequence AS int) ASC
        """
        
        # Get the current time and date
        if not query_date:
            query_date = self.get_date()
        if not query_time:
            query_time = self.get_time()

        limit = 1
        
        # Execute the query with the specified stop_id and current time/date
        cursor.execute(query, (route, direction, query_date, query_time, limit, offset))
                 
        # Fetch all results
        result = cursor.fetchall()
        
        # Close the database connection
        conn.close()
        
        return result
        
    def get_remaining_stops(self, route_id, stop_id, query_date=None, query_time=None):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Define the query
        query = """
        WITH today_trips AS (
            SELECT 
                trips.trip_id, 
                trips.route_id, 
                trips.service_id, 
                trips.direction_id
            FROM 
                trips
            INNER JOIN calendar_dates ON 
                trips.service_id = calendar_dates.service_id
            WHERE 
                trips.route_id = ?
            AND
                calendar_dates.date = ?
        ),
        trip_times AS (
            SELECT 
                st.trip_id,
                st.stop_id,
                st.arrival_time AS next_arrival_time,
                st.stop_sequence
            FROM 
                stop_times st
            INNER JOIN today_trips tt ON 
                st.trip_id = tt.trip_id
        ),
        next_trip AS (
            SELECT 
                tt.trip_id,
                tt.next_arrival_time,
                tt.stop_sequence
            FROM
                trip_times tt
            WHERE
                tt.next_arrival_time > ?
            AND
                tt.stop_id = ?
            ORDER BY 
                tt.next_arrival_time
            LIMIT 1
        ), 
        target_stop_sequence AS (
            SELECT 
                CAST(st.stop_sequence AS int) AS target_sequence
            FROM 
                stop_times st
            WHERE 
                st.stop_id = ?
            LIMIT 1
        )
        
        SELECT 
            st.stop_id, 
            st.stop_sequence,
            stops.stop_name,
            st.arrival_time
        FROM 
            stop_times st
        INNER JOIN stops ON 
            st.stop_id = stops.stop_id
        INNER JOIN next_trip nt ON 
            st.trip_id = nt.trip_id
        WHERE
            CAST(st.stop_sequence AS int) >= (SELECT target_sequence FROM target_stop_sequence)
        ORDER BY
            CAST(st.stop_sequence AS int) ASC;
        """
        
        # Get the current time and date
        if not query_date:
            query_date = self.get_date()
        if not query_time:
            query_time = self.get_time()
        
        # Execute the query with the specified stop_id and current time/date
        cursor.execute(query, (route_id, query_date, query_time, stop_id, stop_id))
        #cursor.execute(query, (current_time))
            
        # Fetch all results
        result = cursor.fetchall()
        
        # Close the database connection
        conn.close()
        
        return result
        
