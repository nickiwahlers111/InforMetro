#! /usr/bin/python3

from database import *

dtrip1 = {'trip_id':[169472306],'route_id':[100],'vehicle_id':[100],'service_key':['Sunday'],'direction':['Out']}
dtrip2 = {'trip_id':[169472307],'route_id':[100],'vehicle_id':[101],'service_key':['Sunday'],'direction':['Out']}
dtrip3 = {'trip_id':[169472308],'route_id':[100],'vehicle_id':[101],'service_key':['Sunday'],'direction':['Out']}

dbread1 = {'tstamp':["2022-05-04 14:30:25"], 'latitude': [45.631987], 'longitude': [-122.640735], 'direction': [11], 'speed':[11], 'trip_id':[169472306] }
dbread2 = {'tstamp':["2022-05-04 14:00:00"], 'latitude': [45.631987], 'longitude': [-122.640735], 'direction': [11], 'speed':[11], 'trip_id':[169472307] }
dbread3 = {'tstamp':["2022-05-04 15:00:00"], 'latitude': [45.631987], 'longitude': [-122.640735], 'direction': [100], 'speed':[11], 'trip_id':[169472307] }

breadframe = []
breadframe.append(dbread1)
breadframe.append(dbread2)
breadframe.append(dbread3)
tripframe = []
tripframe.append(dtrip1)
tripframe.append(dtrip2)
tripframe.append(dtrip3)

print("Calling database_stuff!\n")

conn = open_and_create()
for i in range(3):
  insert_into_table(conn, tripframe[i], breadframe[i])

close_db(conn)

#TODO: currently the data tables get dropped if exist every time database_stuff is called,
#can we call it once outisde of loop in ctran_consumer file?
#do we need to send in a flag? 
