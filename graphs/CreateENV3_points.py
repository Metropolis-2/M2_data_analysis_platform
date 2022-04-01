# -*- coding: utf-8 -*-
"""
Created on Fri Apr  1 10:34:06 2022

@author: nipat
"""

import osmnx as ox
import json
import dill
import shapely.geometry
from pyproj import  Transformer
import math
import geopandas
import geojson



G = ox.io.load_graphml('finalized_graph.graphml')

nodes, edges = ox.graph_to_gdfs(G)

env3_points_dict={} #the dictionary keys arenumbering of teh point, the value is a list [lat,lon,area_string], where area string migh be "constained, "open" or "borders"

points_cnt=0
##go through teh points in constarined
for ii, node in nodes.iterrows():
    env3_points_dict[points_cnt]=[node["y"],node["x"],"constrained"]
    points_cnt+=1
    
    
    
#Load the polygon of the constrained airspace
input_file=open("constrained_poly.dill", 'rb') 
constrained_poly=shapely.geometry.Polygon(dill.load(input_file) )
    
airspace_area=8000*8000*3.1416 #(in m^2) , radius of 8 km
constrained_area=constrained_poly.area
airspace_perimeter=2*8000*3.1416 # in m
square_area=16000*16000

#Compute how many points the whole airspace
number_of_points_total=int((points_cnt+1)*(airspace_area)/constrained_area)

number_of_points_open=int((points_cnt+1)*(airspace_area-constrained_area)/constrained_area)

number_of_points_square=int((points_cnt+1)*(square_area)/constrained_area)
number_of_points_square=pow(int(math.sqrt(number_of_points_square)),2)



##Load teh airspace borders
with open('airspace_border.geojson', 'r') as filename:
    airspace_border= json.load(filename)
f=airspace_border ["features"] [0]["geometry"]["coordinates"][0]      
     
transformer = Transformer.from_crs('epsg:4326','epsg:32633')
ff=[]
fff=[]
for i in range(len(f)-1):
    p1=transformer.transform(f[i][1],f[i][0])
    p2=transformer.transform(f[i+1][1],f[i+1][0])
    ff.append(((p1[0],p1[1]),(p2[0],p2[1])))
    fff.append((p1[0],p1[1]))
    

open_poly=shapely.geometry.Polygon(fff) 
p1=transformer.transform(f[-1][1],f[-1][0])
fff.append((p1[0],p1[1]))
border_linestring=shapely.geometry.LineString(fff) 

center_point=open_poly.centroid

points_grid=[]
x_init=center_point.x-8000
y_init=center_point.y-8000
rows=math.sqrt(number_of_points_square)
dx=16000/rows
dy=16000/math.sqrt(number_of_points_square)
#Create gird with the points
for i in range(number_of_points_square):
    point=shapely.geometry.Point(x_init+dx*(i%rows),y_init+dy*math.floor(i/rows))
    points_grid.append(point)
    

nfz_gdf = geopandas.read_file("geofences_big.gpkg")
nfz_polys=[]
for ii, nfz in nfz_gdf.iterrows():
    polygon=nfz["geometry"]
    nfz_polys.append(polygon.convex_hull)

transformer = Transformer.from_crs('epsg:32633','epsg:4326')

#Add the points which are not in constrained and not in nfzs as "open" points
for p in points_grid:
    in_airspace=open_poly.contains(p)
    in_constrained=constrained_poly.contains(p)
    in_nfz=False
    

    for nfz in nfz_polys:
        if nfz.contains(p):
            in_nfz=True
            print("nfz")
            break

    
    if in_airspace and not in_constrained and not in_nfz:
        geo_p=transformer.transform(p.x,p.y)
        env3_points_dict[points_cnt]=[geo_p[0],geo_p[1],"open"]
        points_cnt+=1
        
number_of_points_border=int(border_linestring.length/math.sqrt(airspace_area/len(env3_points_dict)/3.1416))

border_points=[border_linestring.interpolate(float(n) / number_of_points_border, normalized=True)   for n in range(number_of_points_border + 1)]

for p in border_points:
    x=p.x
    y=p.y
    border_p=transformer.transform(x,y)
    env3_points_dict[points_cnt]=[border_p[0],border_p[1],"border"]
    points_cnt+=1


# Serializing json 
json_object = json.dumps(env3_points_dict, indent = 3)
  
# Writing to sample.json
with open("env3_points.json", "w") as outfile:
    outfile.write(json_object)
    

geojson_features = []
for p in     env3_points_dict.values():
    
    point = geojson.Point((p[1],p[0]))


    geojson_features.append(geojson.Feature(geometry=point, properties={"type": p[2]}))


feature_collection = geojson.FeatureCollection(geojson_features)

with open('env3_points.geojson', 'w') as f:
   geojson.dump(feature_collection, f)
   
# =============================================================================
# with open('env3_points.geojson') as f:
#     gj = geojson.load(f)
# =============================================================================
