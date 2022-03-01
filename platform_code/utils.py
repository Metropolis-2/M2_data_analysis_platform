import time
import geopy.distance
import json
import statistics

class Utils():

    def __init__(self, ):
        self.MINUTS_CONVERSION = 3600
        self.SECONDS_CONVERSION = 60
        self.AIRCRAFT_PATH = "aircraft.json"

    def get_sec(self, time_str):  # Convert hh:mm:ss format to tiemstamp seconds
        h, m, s = time_str.split(':')
        return int(h) * self.MINUTS_CONVERSION + int(m) * self.SECONDS_CONVERSION + int(s)

    def get_min(self, time_int):  # Convert tiemstamp seconds to hh:mm:ss
        timestamp = time.strftime('%H:%M:%S', time.gmtime(time_int))
        return timestamp

    def mergeDict(self, dict1, dict2):
        ''' Merge dictionaries and keep values of common keys in list'''
        dict3 = {**dict1, **dict2}
        for key, value in dict3.items():
            if key in dict1 and key in dict2:
                dict3[key] = value + dict1[key] #ATENTION: #both dict should be like: dict1: [k1] = [v1,v11,...], dict2: [k2] = [v2,...]
        return dict3

    def distCoords(self, coords_1, coords_2): #coords_1=(x1,y1,z1) and coords_2=(x2,y2,z2)
        dst = geopy.distance.distance(coords_1, coords_2).m
        return dst

    def getVehicleMaxSpeed(self, vehicle_type): #vehicle_type will be 'MP20' or 'MP30'
        speed = json.load(open(self.AIRCRAFT_PATH))[vehicle_type]['envelop']['v_max']
        return speed #in m/s

    def stdDeviation_statistics(self, data_list): #return standard deviation of data_list
        return statistics.stdev(data_list)

    def average_statistics(self, data_list): #return average value of data_list
        return sum(data_list)/len(data_list)

    def feetToMeter(self, feet):
        return float(feet/3.281)