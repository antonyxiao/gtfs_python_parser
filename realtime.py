from google.transit import gtfs_realtime_pb2
import requests
import time
import csv
import os

class Realtime:
    def __init__(self, agency, city):
        self.agency = agency
        self.city = city
        
        self.service_alerts_feed = gtfs_realtime_pb2.FeedMessage()
        self.trip_updates_feed = gtfs_realtime_pb2.FeedMessage()
        self.vehicle_positions_feed = gtfs_realtime_pb2.FeedMessage()
        
        # url of transit data does not exist
        if not os.path.isfile('transit_gtfs_list.csv'):
            print('you must include transit_gtfs_list.csv for realtime data.')
        else:
             with open('transit_gtfs_list.csv', newline='') as transit_list:
                spamreader = csv.reader(transit_list, delimiter=',')
                for row in spamreader:
                    if row[0] == self.agency and row[1] == self.city:
                        # 4th column is the service alerts link
                        self.service_alerts_url = row[3]
                        # 5th column is the trip updates link
                        self.trip_updates_url = row[4]
                        # 6th column is the vehicle update link
                        self.vehicle_positions_url = row[5]
    
    def update_service_data(self):
        # get realtime proto file
        response = requests.get(self.service_alerts_url)
        
        # Parse the response into structured dict
        self.service_alerts_feed.ParseFromString(response.content)
                
    def update_trip_data(self):
        # get realtime proto file
        response = requests.get(self.trip_updates_url)
        
        # Parse the response into structured dict
        self.trip_updates_feed.ParseFromString(response.content)
        
    def update_vehicle_data(self):
        # get realtime proto file
        response = requests.get(self.vehicle_positions_url)
        
        # Parse the response into structured dict
        self.vehicle_positions_feed.ParseFromString(response.content)
        
    def get_trip_data(self):
        trips = {}
        
        # go through the realtime object and store vehicle information
        for entity in self.trip_updates_feed.entity:
            
            trip_id = entity.trip_update.trip.trip_id
            stop_time_update = entity.trip_update.stop_time_update
            
            if trip_id not in trips:
                trips[trip_id] = [stop_time_update]
            else:
                trips[trip_id].append(stop_time_update)
        
        return trips

    def get_vehicle_data(self):
        # realtime vehicle information with trip_id as key
        vehicles = {}
        
        # go through the realtime object and store vehicle information
        for entity in self.vehicle_positions_feed.entity:
            trip_id = entity.vehicle.trip.trip_id
            
            # if multiple buses of the same trip exist
            if trip_id not in vehicles:
                vehicles[trip_id] = [entity.vehicle]
            else:
                vehicles[trip_id].append(entity.vehicle)
            
        return vehicles
     





















