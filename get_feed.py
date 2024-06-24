from google.transit import gtfs_realtime_pb2
import requests

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get('https://bct.tmix.se/gtfs-realtime/tripupdates.pb?operatorIds=48')
feed.ParseFromString(response.content)
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print(entity.trip_update.stop_time_update[:2])
    break
