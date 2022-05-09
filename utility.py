#!/usr/bin/env python

def get_trip_id(data):
  id = data['count']['EVENT_NO_TRIP']
  return id

def is_match(current_id, previous_id):
    if previous_id is None:
        return False
    if current_id == previous_id:
        return True
    return False
