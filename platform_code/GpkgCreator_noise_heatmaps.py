# -*- coding: utf-8 -*-
"""
Created on Mon May 16 17:16:33 2022

@author: nipat
"""

import geopandas as gpd
import dill
import math
import json
from shapely.geometry import Point
from pyproj import Transformer
import pandas

gkpg_path="noise_gkpg/"
dills_path="dills/"
reference_noise=73 #dB

transformer = Transformer.from_crs('epsg:4326', 'epsg:32633')

def create_gpkg_noise():

    input_file=open(dills_path+"env3_1_metric_dataframe.dill", 'rb')
    env3_1_metric_dataframe=dill.load(input_file)
    input_file.close()
    
    file_path="data/interest_points.json"
    with open(file_path) as f:
        interest_points = json.load(f)
    
    scenario_names=env3_1_metric_dataframe["Scenario_name"].values
    for scenario in scenario_names:
        print(scenario)
        noise_values=env3_1_metric_dataframe[env3_1_metric_dataframe["Scenario_name"]==scenario]["ENV3_1"].values[0]
  
        noise_db_points=[]
        noise=[]
        for i,n in enumerate(noise_values):
      
            if n >0:
                nn=10*math.log10(n)+reference_noise
                lat,lon,p_type=interest_points[str(i)]
                p1 = transformer.transform(lat, lon)
                pp=Point(p1)
                noise.append(str(nn))
                #pp.noise_level=nn
                noise_db_points.append(pp)
                
        gdf2 = gpd.GeoDataFrame(noise,geometry=noise_db_points ,crs='epsg:32633')

        gdf2.to_file(gkpg_path+scenario+"noise.gpkg", driver="GPKG")
        print(gdf2)


create_gpkg_noise()
