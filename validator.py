from re import A
import pandas as pd

#The velocity of any given bus should not exceed some reasonable value 
# (i.e. Velocity should not exceed 36m/s)
def reasonable_velocity_value(velocity):
    if velocity == "":
        return False
    if int(velocity) > 36:
        return False
    return True

# Every bus’s recorded GPS coordinates are on Earth.
# Latitude: -90 to 90 latitude
def latitude_within_range(lat):
    if lat == '':
        return False
    Lat = float(lat)
    if(Lat >= -90 and Lat <= 90):
        return True
   
    return False

# Every bus’s recorded GPS coordinates are on Earth.
#   -180 to 180 longitude
def longitude_within_range(long):
    if long == '':
        return False
    Long = float(long)
    if(Long >= -180 and Long <= 180):
        return True
    return False

# The total running time of any given bus should not exceed a full day 
# (i.e. The actual time after midnight should not exceed 86400 seconds).
def reasonable_total_time(time):
    if int(time) > 86400:
       return False
    return True

# On a given day, there are hundreds but not thousands of buses running.
def reasonable_bus_count(vehicle_id):
    if vehicle_id.nunique() > 1000:
        return False
    return True

# Information for radio quality should never be available.
def no_radio_quality(radio_qual):
    if(radio_qual == ""):
        return True
    return False

# A bus should always have a direction between 0 and 359, and is not blank.
def correct_direction_value(direction):
    if direction is None or direction == '':
        return False 
    my_direction = int(direction)
    if my_direction > 359 and my_direction < 0:
        return False
    return True

# Every bus has a trip ID.
def has_trip_id(id):
    if id is None:
        return False
    else: 
        return True

# A bus may be early or late by an amount of minutes, but not hours.
def reasonable_schedule_deviation(seconds):
    if seconds is None or seconds == '':
        return False
    if abs(int(seconds)) > 3600:
        return False
    return True

#Each bus has an operation date
def has_op_date(op_date):
    if op_date == '':
        return False
    return True

def has_GPS_HDOP(hdop):
    if hdop == '':
        return False
    else:
        return True

def validate_breadcrumb(breadcrumb):
    if(reasonable_velocity_value(breadcrumb['VELOCITY']) and
        latitude_within_range(breadcrumb['GPS_LATITUDE']) and
        longitude_within_range(breadcrumb['GPS_LATITUDE']) and
        reasonable_total_time(breadcrumb['ACT_TIME']) and
        correct_direction_value(breadcrumb['DIRECTION']) and
        has_trip_id(breadcrumb['EVENT_NO_TRIP']) and
        reasonable_schedule_deviation(breadcrumb['EVENT_NO_TRIP']) and
        has_op_date(breadcrumb['OPD_DATE']) and
        has_GPS_HDOP(breadcrumb['GPS_HDOP'])):
        return True
    return False


def validate_vehicle_number(vn):
    if vn == '':
        return False
    return True

def validate_route_number(rn):
    if rn == '':
        return False
    return True

def validate_direction(d):
    if d != '0' and d != '1':
        return False
    return True

def validate_service_key(sk):
    if sk != 'W' and sk != 'U' and sk != 'A' and sk != 'H':
        return False
    return True

def validate_stop_event(stopevent):
    if(validate_vehicle_number(stopevent['vehicle_number']) and
        validate_route_number(stopevent['route_number']) and
        validate_direction(stopevent['direction']) and
        validate_service_key(stopevent['service_key'])):
        return True
    return False
