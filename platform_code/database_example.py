# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 16:33:17 2022

@author: nipat
"""
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession

import BaselineComputations
import ComputeMetrics

##Read the log file
concept="3" ##DECENTRALISED
density="very_low"
distribution="40"
repetition="8"
uncertainty="W1"
scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty


    

##Init    
spark = SparkSession.builder.appName('Platform Analysis.com').getOrCreate()
sparkContext=spark.sparkContext


def create_loslog_dataframe():
    ##LOSLOG dataframe
    
    loslog_file=open("example_logs/LOSLOG_Flight_intention_very_low_40_8_W1_20220201_17-13-56.log","r")
    
    loslog_list=list()
    cnt=0
    for line in loslog_file:
        cnt=cnt+1
        if cnt<10:
            continue
        line_list=line.split(",")
        tmp_list=[cnt-10,scenario_name]
        for iv,value in enumerate(line_list):
            if iv<3 or (iv >4 and iv <11):
                tmp_list.append(float(value))
            elif iv==11:
                tmp_list.append(float(value[:-2]))  
            else:
                tmp_list.append(value)
    
        loslog_list.append(tmp_list)
    
    col_list=["LOS_id","Scenario_name","LOS_exit_time","LOS_start_time","Time_of_min_distance","ACID1","ACID2","LAT1","LON1","ALT1","LAT2","LON2","ALT2","DIST"]
    
    df = pd.DataFrame(loslog_list,columns=col_list )
    
    loslog_data_frame=spark.createDataFrame(df)
    
    loslog_data_frame.show()
    return loslog_data_frame
    
    ####

##CONFLOG dataframe
def create_conflog_dataframe():
    conflog_file=open("example_logs/CONFLOG_Flight_intention_very_low_40_8_W1_20220201_17-13-56.log","r")
    
    conflog_list=list()
    cnt=0
    for line in conflog_file:
        cnt=cnt+1
        if cnt<10:
            continue
        line_list=line.split(",")
        tmp_list=[cnt-10,scenario_name]
        for iv,value in enumerate(line_list):
            if (iv==0 or iv >2) and iv !=7:
                tmp_list.append(float(value))
            elif iv==10:
                tmp_list.append(float(value[:-2]))  
            else:
                tmp_list.append(value)
    
        conflog_list.append(tmp_list)
    
    col_list=["CONF_id","Scenario_name","CONF_detected_time","ACID1","ACID2","LAT1","LON1","ALT1","LAT2","LON2","ALT2","CPALAT","CPALON"]
    
    df = pd.DataFrame(conflog_list,columns=col_list )
    
    conflog_data_frame=spark.createDataFrame(df)
    
    conflog_data_frame.show()
    return conflog_data_frame

####

##GEOLOG dataframe
def create_geolog_dataframe():
    geolog_file=open("example_logs/GEOLOG_Flight_intention_very_low_40_8_W1_20220201_17-13-56.log","r")
    
    geolog_list=list()
    cnt=0
    for line in geolog_file:
        cnt=cnt+1
        if cnt<10:
            continue
        line_list=line.split(",")
        tmp_list=[cnt-10,scenario_name]
        for iv,value in enumerate(line_list):
            if (iv==0 or iv >3) and iv!=7:
                tmp_list.append(float(value))
            elif iv==7:
                tmp_list.append(float(value[:-2]))  
            else:
                tmp_list.append(value)
    
        geolog_list.append(tmp_list)
    
    col_list=["GEO_id","Scenario_name","Deletion_time","ACID","GEOF_ID","GEOF_NAME","MAX_intrusion","LAT_intrusion","LON_intrusion","intrusion_time"]
    
    df = pd.DataFrame(geolog_list,columns=col_list )
    
    geolog_data_frame=spark.createDataFrame(df)
    
    geolog_data_frame.show()
    return geolog_data_frame

####


##FLSTLOG dataframe
def create_flstlog_dataframe():
    flstlog_file=open("example_logs/FLSTLOG_Flight_intention_very_low_40_8_W1_20220201_17-13-56.log","r")
    
    flstlog_list=list()
    cnt=0
    for line in flstlog_file:
        cnt=cnt+1
        if cnt<10:
            continue
        line_list=line.split(",")
        tmp_list=[cnt-10,scenario_name]
        for iv,value in enumerate(line_list):
            if (iv==0 or iv >3 )and iv !=7:
                tmp_list.append(value)
            elif iv==7:
                tmp_list.append(value)  
            else:
                tmp_list.append(value)
    
        flstlog_list.append(tmp_list)
    
    col_list=['Flight_id',"scenario_name","ACID","Origin","Dest","Baseline_deparure_time","Aircarft_type","Priority","Loitering","Baseline_2d_path_length","Baseline_3d_path_length",\
              "Baseline_vertical_path_length","Baseline_flight_duration","DEL_time","SPAWN_time","FLIGHT_time","2D_dist","3D_dist","ALT_dist","Work_done","DEL_LAT"\
              ,"DEL_LON","DEL_ALT","TAS","DEL_VS","DEL_HDG","ASAS_active","PILOT_ALT","PILOT_SPD","PILOT_HDG","PILOT_VS"]
    
    df = pd.DataFrame(flstlog_list,columns=col_list )
    
    flstlog_data_frame=spark.createDataFrame(df)
    
    flstlog_data_frame.show()
    return flstlog_data_frame

####

##FLSTLOG dataframe
def create_reglog_dataframe():
    reglog_file=open("example_logs/REGLOG_Flight_intention_very_low_40_8_W1_20220201_17-13-56.log","r")
    
    reglog_list=list()
    cnt=0
    for line in reglog_file:
        cnt=cnt+1
        if cnt<10:
            continue
        line_list=line.split(",")
        tmp_list=[cnt-10,scenario_name,scenario_name+line_list[1]]
        tmp_list.append(float(line_list[0]))
        aircraft_num=0
        acid_list=[]
        alt_list=[]
        lat_list=[]
        lon_list=[]
        alt_cnt=0
        lat_cnt=0
        lon_cnt=0
        for iv,value in enumerate(line_list):
            if iv==0:
                continue
            if value[0]=="D":
                aircraft_num=aircraft_num+1
                acid_list.append(value)
            elif alt_cnt<aircraft_num:
                alt_list.append(float(value))
                alt_cnt=alt_cnt+1
            elif lat_cnt<aircraft_num:
                lat_list.append(float(value))
                lat_cnt=lat_cnt+1     
            elif lon_cnt<aircraft_num-1:
                lon_list.append(float(value))
                lon_cnt=lon_cnt+1 
            else:
                lon_list.append(float(value[:-2]))
    
        tmp_list.append(aircraft_num)
        tmp_list.append(acid_list)
        tmp_list.append(alt_list)
        tmp_list.append(lat_list)  
        tmp_list.append(lon_list)
        reglog_list.append(tmp_list) #pyspark gives an error when trying to pass list as a value
    
    col_list=['REG_id',"scenario_name","Time","Aicraft_num","Flight_name","ACID","ALT","LAT","LON"]
    
    df = pd.DataFrame(reglog_list,columns=col_list )
    
    reglog_data_frame=spark.createDataFrame(df)
    
    reglog_data_frame.show()
    return reglog_data_frame

####
##FLSTLOG dataframe
def create_reglog_grouped_dataframe():
    reglog_grouped_data_frame=1
    return reglog_grouped_data_frame

####
##metrics dataframe
def create_metrics_dataframe():
    
    metrics_list=list()
    tmp_list=[scenario_name,"-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-","-"]
    metrics_list.append(tmp_list)

    
    col_list=["Scenario_name","Aircraft_number","AEQ1","AEQ1_1","AEQ2","AEQ2_1","AEQ3","AEQ4","AEQ5","AEQ5_1","CAP1","CAP2","CAP3","CAP4","ENV1","ENV2","ENV3","ENV4","SAF1","SAF2","SAF3","SAF4","SAF5","SAF6"\
              "EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","EFF7","EFF8","PRI1","PRI2","PRI3","PRI4","PRI5"]
    
    df = pd.DataFrame(metrics_list,columns=col_list )
    
    metrics_data_frame=spark.createDataFrame(df)
    
    metrics_data_frame.show()
    return metrics_data_frame

####

loslog_dataframe=create_loslog_dataframe()
conflog_dataframe=create_conflog_dataframe()
geolog_dataframe=create_geolog_dataframe()

metrics_dataframe=create_metrics_dataframe()

