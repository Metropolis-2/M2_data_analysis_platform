# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 19:25:26 2022

@author: labpc2
"""
import geopy.distance

FEET_TO_METERS_SCALE=0.3048
def get_coordinates_distance(origin_latitude: float, origin_longitude: float,
                             destination_latitude: float, destination_longitude: float) -> float:
    """ Calculates the distance in meters between two world coordinates.

    :param origin_latitude: origin latitude point.
    :param origin_longitude: origin longitude point.
    :param destination_latitude: destination latitude point.
    :param destination_longitude: destination longitude point.
    :return: distance in meters.
    """
    origin_tuple = (origin_latitude, origin_longitude)
    destination_tuple = (destination_latitude, destination_longitude)
    return geopy.distance.distance(origin_tuple, destination_tuple).m

def convert_feet_to_meters(distance_in_feet):
    """ Converts a given column that contains the altitude in feets to meters.

    """
    return distance_in_feet* FEET_TO_METERS_SCALE


drone_vertical_speed=5 #5 m/s

mp20_cruisng_speed=10.29 #m/s
mp30_cruisng_speed=15.43 #m/s


def compute_work_done(ascend_dist,flight_time):
    
    work_done=flight_time+ascend_dist/drone_vertical_speed
    return work_done


def compute_ascending_distance(vertical_distance,deletion_altitude):
    ascend_dist=deletion_altitude+(vertical_distance-deletion_altitude)/2
    return ascend_dist


def compute_baseline_vertical_distance(loitering):
    if loitering:
        baseline_vertical_distance=9.144 # 30 feet
    else:
        baseline_vertical_distance=0
    return baseline_vertical_distance

def compute_baseline_ascending_distance():
    baseline_ascending_distance=9.144 # 30 feet
    return baseline_ascending_distance

