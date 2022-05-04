from database import database_stuff

dtrip = {'trip_id':[169472306],'route_id':[100],'vehicle_id':[100],'service_key':['Sunday'],'direction':['Out']}
dbread = {'tstamp':["2022-05-04 14:30:25"], 'latitude': [45.631987], 'longitude': [-122.640735], 'direction': [11], 'speed':[11], 'trip_id':[169472306] }
print("Calling database_stuff!\n")
database_stuff(dtrip, dbread)

#TODO: currently the data tables get dropped if exist every time database_stuff is called,
#can we call it once outisde of loop in ctran_consumer file?
#do we need to send in a flag? 
