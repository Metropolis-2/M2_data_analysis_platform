# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 19:18:10 2022

@author: labpc2
"""
import pandas as pd
import json
import util_functions
import math

from pyproj import Transformer

env3_radius_threshold=16 # in meters
env3_reference_altitude=9.144 # 30 feet

sound_exposure_threshold=0.01517 #TODO rethink that value

transformer = Transformer.from_crs('epsg:4326', 'epsg:32633')

def load_interest_points() :
    """ Parses the JSON file with the interest points. The returned
    object is a dictionary with the name of the point as the key and
    a list as value with the latitude, longitude and the type of point.

    The point could be of type:
     - 'constained': if it is inside the city area.
     - 'border': if it is the border between the city area and the open area.
     - 'open': if it is in the open area.

    :param file_path: path to the interest points JSON file.
    :return: dictionary with the point name as key and
     [latitude, longitude, type] as value.
    """
    file_path="data/interest_points.json"
    
    with open(file_path) as f:
        interest_points = json.load(f)
    return interest_points


def compute_env1(df):
    """ ENV-1: Work done

    Representing total energy needed to perform all flight intentions,
    computed by integrating the thrust (force) over the route displacement.

    :param input_dataframes: filter by scenario flst_dataframe.
    :return: teh computed ENV1 metric.
    """
    df_filtered=df[df['Spawned']&(df['ALT_dist']!=-1)]
    env1=df_filtered["work_done"].sum()
    
    return env1


def compute_eucledean_distance(lat1,lon1,lat2,lon2):
    p1 = transformer.transform(lat1, lon1)
    p2 = transformer.transform(lat2, lon2)
    return math.sqrt((p1[0]-p2[0])*(p1[0]-p2[0])+(p1[1]-p2[1])*(p1[1]-p2[1]))


def compute_env3_1(i_scpt_list,number_of_time_steps):
    """ ENV-3_1:  Equivalent Noise Level

    Represent total sound exposure at the given point on city area surface.
    It is computed by aggregating the total sound intensity (of all sound sources)
    at that given point over the time.

    :param input_dataframes: a list with the computed sound exposure of each pointof interset for all time_stamps
    :return: the computed ENV3 metric for each node in constarined.
    """
    env3_1=[0]*len(i_scpt_list[0])   
    for row in i_scpt_list:
        for i in range(len(row)):
            env3_1[i]+=row[i]

    

    for i in range(len(env3_1)):
        if env3_1[i]==0:
            a=1
            #print("no exposure")
            
        else:
            env3_1[i]/=number_of_time_steps
            #env3_1[i]=10*math.log10(env3_1[i])
            #print(env3_1[i])
 
    return env3_1

def compute_env3_2(i_scpt_list):
    """ ENV-3_2:  number of points exposed to more than threshold noise at a given momment

    Represent total sound exposure at the given point on city area surface.
    It is computed by aggregating the total sound intensity (of all sound sources)
    at that given point over the time.

    :param input_dataframes: a list with the computed sound exposure of each pointof interset for all time_stamps
    :return: the computed ENV3 metric for each node in constarined.
    """
    env3_2=0  
    for row in i_scpt_list:
        for j in row:
            if j >sound_exposure_threshold:
                env3_2+=1

    return env3_2    
    
def compute_i_scpt(positions_list):
    """ Compute the sound intesnsity for a given time stamp

    :param input_dataframes: a list with teh positions of all aircarftas at that time_stamp, each elemnet of the list is a list [lat,lon,alt], alt in ft
    :return: the computed sound intensity  for each node in constarined.
    """
    
    delta_lat = 1/(60*5) #1 minute of degree = 1NM (sep norm is 5NM)
    delta_lon = 1/(60*5) #hat is about 300 meters 
    #grid filling
    grid = {}
    for p in positions_list:
        ind_lat = int(p[0]/delta_lat+0.5)
        ind_lon =int( p[1]/delta_lon+0.5)
        key = str(ind_lat)+"-"+str( ind_lon)
        if key not in grid.keys():
            grid[key]=[]
        grid[key].append(p)
        
 
    
    i_scpt=[]

    
    interest_points = load_interest_points()
    cnt=0
    for name, (latitude, longitude, point_type) in interest_points.items():
        cnt+=1
        if 0:
            continue
        if point_type!="constrained":
            print("Wrong type",point_type)
            continue
        ind_lat = int(latitude/delta_lat+0.5)
        ind_lon =int( longitude/delta_lon+0.5)
        possible_noise_sources=[]
        for i in range(ind_lat-1,ind_lat+2):
            for j in range(ind_lon-1,ind_lon+2):
                key = str(i)+"-"+str(j)
                if key in grid.keys():
                    
                    possible_noise_sources=possible_noise_sources+grid[key]

        i_scpt_tmp=0
        for pos in possible_noise_sources:
            lat=pos[0]
            lon=pos[1]
            dist=compute_eucledean_distance(lat, lon, latitude, longitude)
            if dist<env3_radius_threshold:
                alt=util_functions.convert_feet_to_meters(pos[2])
                if alt==0:
                    alt=0.1
                i_scpt_aircarft=env3_reference_altitude*env3_reference_altitude/(alt*alt)
                i_scpt_tmp+=i_scpt_aircarft
 

        i_scpt.append(i_scpt_tmp)
  
    return i_scpt

