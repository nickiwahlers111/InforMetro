from re import A
import pandas as pd

# Transforming VELOCITY (m/s) to Speed (mph)
def velocity_to_speed(velocity):
    if velocity == "":
        velocity = 0
    velocity = float(velocity)
    speed = (velocity * 3600) / 1609.34
    return round(speed, 2)


# ACT_TIME (NUMBER) from seconds, to h:m:s
def convert_seconds(time):
    t = int(time)
    s = t % 60
    hold = (t - s) / 60 # leftover minutes
    m = hold % 60 
    h = (hold - m) / 60
    if h >= 24:
        h=h-24
    result = '{:d}:{:02d}:{:02d}'.format(int(h),int(m),int(s))
    return result

def direction_to_value(direction):
    if(direction == ""):
        direction = -1
    return direction

def coordinate_to_value(coordinate):
    if(coordinate == ""):
        coordinate = -1
    return coordinate


###############################################################################################################################

def transform(data):
    # df = pd.DataFrame([data])
    df = data

    # Breadcrumb: tstamp, latitude, longitude, direction, speed, trip_id
    breadcrumb = df[['OPD_DATE', 'ACT_TIME', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'DIRECTION', 'VELOCITY', 'EVENT_NO_TRIP']].rename({
        'OPD_DATE':'date', 'ACT_TIME':'time', 'GPS_LATITUDE':'latitude', 'GPS_LONGITUDE':'longitude', 'DIRECTION':'direction', 
        'VELOCITY':'speed', 'EVENT_NO_TRIP':'trip_id'}, axis=1)
    
    # Trip: trip_id, route_id, vehicle_id, service_key, direction
    trip = df[['EVENT_NO_TRIP', 'VEHICLE_ID']].rename({
        'EVENT_NO_TRIP':'trip_id', 'VEHICLE_ID':'vehicle_id'}, axis=1
    )
    
    # NOTE: We do not have enough information to populate route_id, service_key, or direction yet.
    # Set to null for now (Project Assignment 2)
    trip['route_id'] = -1
    trip['service_key'] = 'Weekday'
    trip['direction'] = 'Out'

    # Transform velocity (m/s) to speed (mph)
    velocity_df = df['VELOCITY']
    speeds = velocity_df.apply(velocity_to_speed)
    breadcrumb['speed'] = speeds

    # Transform blank directions to NULL
    breadcrumb['direction'] = breadcrumb['direction'].apply(direction_to_value)

    # Transform blank latitudes and longitudes to NULL
    breadcrumb['latitude'] = breadcrumb['latitude'].apply(coordinate_to_value)
    breadcrumb['longitude'] = breadcrumb['longitude'].apply(coordinate_to_value)

    # Transform OPD_DATE and ACT_TIME to a single timestamp

    dates = df['OPD_DATE']
    times = df['ACT_TIME']
    times = times.apply(convert_seconds)       
    datetimes = pd.concat([dates, times], axis=1)

    datetimes['time_stamp'] = datetimes['OPD_DATE'] + ' ' + datetimes['ACT_TIME']
    breadcrumb['tstamp'] = datetimes['time_stamp']
    breadcrumb = breadcrumb.drop(columns=['date', 'time'])

    return trip, breadcrumb


###############################################################################################################################

def transform_direction(direction):
    if(direction == '0'):
        direction = 'Out'
    else:
        direction = 'In'
    return direction


def transform_service_key(key):
    if(key == 'W'):
        key = 'Weekday'
    elif(key == 'A'):
        key = 'Saturday'
    else:
        key = 'Sunday'
    return key


def transform_stop_event(data):
    df = data

    stopevent = df[['vehicle_number', 'route_number', 'service_key', 'direction']].rename({
        'vehicle_number':'vehicle_id', 'route_number':'route_id'}, axis = 1)

    stopevent['direction'] = stopevent['direction'].apply(transform_direction)
    stopevent['service_key'] = stopevent['service_key'].apply(transform_service_key)

    return stopevent
