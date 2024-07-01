from gtfs import GTFS
import time
import datetime
# IOS only module
import location
from realtime import Realtime

victoria = GTFS('bctransit', 'victoria', update_db=False)

# get current location coordinates from the IOS location module
def get_location():
    location.start_updates()
    loc = location.get_location()
    location.stop_updates()
    if loc:
        return {'lat': loc['latitude'], 'lon': loc['longitude']}

def get_vehicle():
    rt = Realtime('bctransit','victoria')
    rt.update_vehicle_data()
    vehicle = rt.get_vehicle_data()
    
    return vehicle

def get_trip():
    rt = Realtime('bctransit', 'victoria')
    rt.update_trip_data()
    trip = rt.get_trip_data()
    
    return trip


def print_nearby_stops_and_buses():
    loc = get_location()
    nearby = victoria.get_nearby_bus_stops(loc['lon'], loc['lat'], radius_km=1, limit=9)
       
    # realtime vehicle position and trip update data
    vehicle = get_vehicle()
    trip = get_trip()
       
    for stop in nearby:
        print(f'[{stop[0]}] {stop[1]} ({stop[4]*1000:.0f}m)')
        incoming = victoria.get_incoming_buses(stop[0], count=5)    
        for bus in incoming:
            print(f'[{bus[1][:-4]}]', end='')
            
            if len(bus[1][:-4]) == 1:
            	print(' ', end='')
            	
            print(f' {bus[0][:-3]}', end='')
            
            if bus[3] in trip:
                for trip_stops in trip[bus[3]]:
                    got_time = False
                    for trip_stop in trip_stops:
                        if stop[0] == trip_stop.stop_id and trip_stop.arrival.delay > 0:
                            # get arrival time in epoch
                            epoch_time = trip_stop.arrival.time
                            
                            # convert epoch to HH:MM time
                            time_from_epoch = datetime.datetime.fromtimestamp(epoch_time).strftime('%H:%M')
                            
                            if time_from_epoch == bus[0][:-3]:
                            	break
                            
                            print(f' → {time_from_epoch}',end='')
                            got_time = True
                            break
                        elif stop[0] == trip_stop.stop_id:
                            print(f'        ',end='')
                            got_time = True
                            break
                    if not got_time:
                        print(f'        ',end='')
            else:
                print(f'        ',end='')

            print(f' {bus[2]}', end='')
            print()
                
            
        print()


print_nearby_stops_and_buses()

#incoming_buses = victoria.get_incoming_buses('101107', query_time='11:20:00', count=3)

#all_trip_stops = victoria.get_all_trip_stops('26-VIC', 1)
#[print(stop) for stop in all_trip_stops]

#reminaing_stops = victoria.get_remaining_stops('26-VIC', '100509')
#[print(stop) for stop in reminaing_stops]

#print(nearest)
#for bus in incoming_buses:
#    print(f"Arrival Time: {bus[0]}, Route: {bus[1]}, {bus[2]}, trip_id: {bus[3]}")
