import sqlite3
from datetime import datetime

db_path = 'sqlite3_db/bctransit_victoria.db'

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
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

now = datetime.now()
current_date = now.strftime('%Y%m%d')
current_time = now.strftime('%H:%M:%S')

route = '26-VIC'
stop_id = '100988'

# Execute the query with the specified stop_id and current time/date
cursor.execute(query, (route, current_date, current_time, stop_id, stop_id))
#cursor.execute(query, (current_time))
    
# Fetch all results
result = cursor.fetchall()

# Close the database connection
conn.close()

#result = Sort(result)

#print(incoming_buses)
for stop in result:
    print(stop)
    #print(f"{stop[0]} {stop[1]} {stop[2]} {stop[3]}")
