import time
import geopy.distance
import json
import statistics
import dill
import shapely 
from pyproj import  Transformer

class Utils():

    def __init__(self, ):
        self.MINUTS_CONVERSION = 3600
        self.SECONDS_CONVERSION = 60
        self.AIRCRAFT_PATH = "aircraft.json"
        
        input_file=open("constrained_poly.dill", 'rb')
        constrained_poly=dill.load(input_file)
        self.constrained_poly=shapely.geometry.Polygon(constrained_poly)

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
        dst = geopy.distance.distance(coords_1, coords_2).m #TODO: direct distance calculation (in meters) between two points, is this approach correct?
        return dst

    def getVehicleMaxSpeed(self, vehicle_type): #vehicle_type will be 'MP20' or 'MP30'
        speed = json.load(open(self.AIRCRAFT_PATH))[vehicle_type]['envelop']['v_max'] #TODO: v_max o cruising_speed
        return speed #in m/s

    def stdDeviation_statistics(self, data_list): #return standard deviation of data_list
        return statistics.stdev(data_list) #TODO: check if is correct: https://docs.python.org/3/library/statistics.html

    def average_statistics(self, data_list): #return average value of data_list
        return sum(data_list)/len(data_list)

    def feetToMeter(self, feet):
        return float(feet/3.281)
    
    #Function to check if a point (lon,lat) is in constrained airspace
    #returns true if point is in contsrained
    #returns false if poitn is in open
    def inConstrained(self,point):
        transformer = Transformer.from_crs('epsg:4326','epsg:32633')
        p=transformer.transform( point[1],point[0])
        p = shapely.geometry.Point(p[0],p[1])
        return self.constrained_poly.contains(p)
    
    #Function to check if the linesegment connecting point1 (lon1,lat1) and point2(lon2,lat2) intersects with the constrained airspace
    #returns true if it intersects, otehrwise it returns false
    def lineIntersects_Constarined(self,point1,point2):
        transformer = Transformer.from_crs('epsg:4326','epsg:32633')
        p1=transformer.transform( point1[1],point1[0])
        p2=transformer.transform( point2[1],point2[0])
        lineSegment = shapely.geometry.LineString([shapely.geometry.Point(p1[0], p1[1]), shapely.geometry.Point(p2[0], p2[1])])
        return lineSegment.intersects(self.constrained_poly)