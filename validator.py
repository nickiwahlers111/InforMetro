from re import A
import pandas as pd

#The velocity of any given bus should not exceed some reasonable value 
# (i.e. Velocity should not exceed 36m/s)
def reasonable_velocity_value(velocity):
    if int(velocity) > 36:
        print("ASSERTION FAILED: Velocity exceeds reasonable value.\n")
        return False
    return True

# Every bus has some recorded velocity. 
# (i.e. The recorded velocity is not “blank”).
def velocity_not_blank(velocity):
    if velocity == "":
        velocity = 0
    return velocity

# Every bus’s recorded GPS coordinates are on Earth.
# Latitude: -90 to 90 latitude
def latitude_within_range(lat):
    if lat == '':
        print("ASSERTION FAILED: Latitude is blank.")
        return False
    Lat = float(lat)
    if(Lat >= -90 and Lat <= 90):
        return True
    print("ASSERTION FAILED: Latitude not within bounds of Earth.\n")
    return False

# Every bus’s recorded GPS coordinates are on Earth.
#   -180 to 180 longitude
def longitude_within_range(long):
    if long == '':
        print("ASSERTION FAILED: Longitude is blank.")
        return False
    Long = float(long)
    if(Long >= -180 and Long <= 180):
        return True
    print("ASSERTION FAILED: Longitude not within bounds of Earth.\n")
    return False

# The total running time of any given bus should not exceed a full day 
# (i.e. The actual time after midnight should not exceed 86400 seconds).
def reasonable_total_time(time):
    if int(time) > 86400:
       print("ASSERTION FAILED: Time exceeds a full day.\n") 
       return False
    return True

# On a given day, there are hundreds but not thousands of buses running.
def reasonable_bus_count(vehicle_id):
    if vehicle_id.nunique() > 1000:
        print("ASSERTION FAILED: there are more than 1000 buses") 
        return False
    return True

# Information for radio quality should never be available.
def no_radio_quality(radio_qual):
    if(radio_qual == ""):
        return True
    print("ASSERTION FAILED: Radio quality field is populated.") 
    return False

# A bus should always have a direction between 0 and 359, and is not blank.
def correct_direction_value(direction):
    if direction is None or direction == '':
        print("ASSERTION FAILED: Direction is blank ")
        return False 
    
    my_direction = int(direction)
    if my_direction > 359 and my_direction < 0:
        print("ASSERTION FAILED: direction outside direction bounds")
        return False
    return True



# Every bus has a trip ID.
def has_trip_id(id):
    if id is None:
        print("ASSERTION FAILED: ID is blank")
        return False
    else: 
        return True

# A bus may be early or late by an amount of minutes, but not hours.
def reasonable_schedule_deviation(seconds):
    if seconds is None or seconds == '':
        print("ASSERTION FAILED: SCHEDULE_DEVIATION is blank")
        return False
    if abs(int(seconds)) >3600:
        print("ASSERTION FAILED: Schedule deviation is greater than an hour")
        return False
    return True


#Each bus has an operation date
def has_op_date(op_date):
    if op_date == '':
        print("ASSERTION FAILED: Operation date is empty")
        return False
    else:
        return True




def do_validate(breadcrumb):
    breadcrumb['VELOCITY'] = velocity_not_blank(breadcrumb['VELOCITY'])
    breadcrumb['VELOCITY'] = reasonable_velocity_value(breadcrumb['VELOCITY'])
    breadcrumb['GPS_LATITUDE'] = latitude_within_range(breadcrumb['GPS_LATITUDE'])
    breadcrumb['GPS_LONGUTUDE'] = longitude_within_range(breadcrumb['GPS_LATITUDE'])
    breadcrumb['ACT_TIME'] = reasonable_total_time(breadcrumb['ACT_TIME'])
    #breadcrumb[''] = (breadcrumb['']) reasonable bus count
    breadcrumb['DIRECTION'] = correct_direction_value(breadcrumb['DIRECTION'])
    breadcrumb['EVENT_NO_TRIP'] = has_trip_id(breadcrumb['EVENT_NO_TRIP'])
    breadcrumb['EVENT_NO_TRIP'] = reasonable_schedule_deviation(breadcrumb['EVENT_NO_TRIP'])
    breadcrumb['OPD_DATE'] = has_op_date(breadcrumb['OPD_DATE'])

