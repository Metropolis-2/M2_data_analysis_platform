

# %%
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 11:47:30 2021
@author: andub
"""
import numpy as np
import os
from os import path
import osmnx as ox
import geopandas as gpd
import numpy as np
from route_computations import BaselineRoutes
import dill
from pyproj import  Transformer
import math
from shapely.geometry import LineString



def kwikdist(origin, destination):
    """
    Quick and dirty dist [nm]
    In:
        lat/lon, lat/lon [deg]
    Out:
        dist [nm]
    """
    # We're getting these guys as strings
    lona = float(origin[0])
    lata = float(origin[1])

    lonb = float(destination[0])
    latb = float(destination[1])

    re      = 6371000.  # radius earth [m]
    dlat    = np.radians(latb - lata)
    dlon    = np.radians(((lonb - lona)+180)%360-180)
    cavelat = np.cos(np.radians(lata + latb) * 0.5)

    dangle  = np.sqrt(dlat * dlat + dlon * dlon * cavelat * cavelat)
    dist    = re * dangle
    return dist

def PreGeneratedPaths():
    """ Generates a set of pre-generated paths for the aircraft. It reads the origins and 
    destinations and creates the list of tuples with
    (origin_lon, origin_lat, destination_lon, destination_lat)
    """
    origins = gpd.read_file('Sending_nodes.gpkg').to_numpy()[:,0:2]
    destinations = gpd.read_file('Recieving_nodes.gpkg').to_numpy()[:,0:2]

    pairs = []
    round_int = 10
    for origin in origins:
        for destination in destinations:
            if kwikdist(origin, destination) >=800:
                lon1 = origin[0]
                lat1 = origin[1]
                lon2 = destination[0]
                lat2 = destination[1]
                pairs.append((round(lon1,round_int),round(lat1,round_int),round(lon2,round_int),round(lat2,round_int)))

    return pairs

# Get Origin-Destination pairs list of all sending receiving nodes
# data is organized as (lon, lat, lon, lat)
pairs_list =PreGeneratedPaths()


print(len(pairs_list), "to be computed")


# =============================================================================
# pairs_list=[[16.359164666,48.2332003055,16.3649540409,48.2267581818]]
# baseline_len_calc=BaselineRoutes()
# length=baseline_len_calc.compute_len(pairs_list[0])
# =============================================================================




baseline_length_dict={}
baseline_len_calc=BaselineRoutes()
cnt=0
for p in pairs_list:
    cnt+=1
    key= str(p[1])+"-"+str(p[0])+"-"+str(p[3])+"-"+str(p[2])
    
    length=baseline_len_calc.compute_len(p)
    baseline_length_dict[key]=length
    if length <800:
        print("Too short!!!!", p)
    if cnt%500 ==0:
        print(cnt,length)
    
    
# =============================================================================
output_file=open("data/baseline_routes.dill", 'wb')
dill.dump(baseline_length_dict,output_file)
output_file.close()
# =============================================================================
  
#Load the open airspace grid
# =============================================================================
input_file=open("data/baseline_routes.dill", 'rb')
diction=dill.load(input_file)
input_file.close()
transformer = Transformer.from_crs('epsg:4326','epsg:32633')
toot_short_cnt=0
smaller_cnt=0
smallest=100000
longest=0
cnt=0
for key in diction.keys():
    coord=list(key.split("-"))
    start=transformer.transform(float(coord[0]),float(coord[1]))
    dest=transformer.transform(float(coord[2]),float(coord[3]))
    d=math.sqrt((start[0]-dest[0])*(start[0]-dest[0])+(start[1]-dest[1])*(start[1]-dest[1]))
    line=LineString([(start[0],start[1]),(dest[0],dest[1])])
    l=diction[key]
    if l<800:
        print(line)
        length=baseline_len_calc.compute_len([float(coord[1]),float(coord[0]),float(coord[3]),float(coord[2])])
        print("Too short",key,l,d,line.length,length)
        toot_short_cnt+=1
    if l<d:
        cnt+=1
        if cnt< 20:

            length=baseline_len_calc.compute_len([float(coord[1]),float(coord[0]),float(coord[3]),float(coord[2])])
            print("eucleadean",l,d,length)
        #diction[key]=length

	#length=baseline_len_calc.compute_len(coord[0],coord[1],coord[2],coord[3])
        #print("Smaller than eucleadean",key)
        smaller_cnt+=1
    if l<smallest:
        smallest=l
    if l>longest:
        longest=l

print(toot_short_cnt)
print(smaller_cnt)
print(smallest)
print(longest)
# =============================================================================
#output_file=open("data/baseline_routes.dill", 'wb')
#dill.dump(diction,output_file)
#output_file.close()
