# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 17:42:34 2022

@author: jpedrero
"""
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when
from pyproj import  Transformer
import os

path_to_logs="example_logs/"

class DataframeCreator():

    def __init__(self,  spark):

        self.spark = spark
        self.centralised_log_names=[]
        self.decentralised_log_names=[]
        self.hybrid_log_names=[]
        self.flight_intention_names=[]
        self.get_file_names()
        
        self.los_log_dataframe = self.create_loslog_dataframe() 
        self.conf_log_dataframe = self.create_conflog_dataframe() 
        self.geo_log_dataframe = self.create_geolog_dataframe()
        self.flst_log_dataframe = self.create_flstlog_dataframe() 
        self.reg_log_dataframe = self.create_reglog_dataframe()
        self.time_log_dataframe=self.create_time_object_dataframe() 
        self.metrics_dataframe=self.create_metrics_dataframe()
        return
    
    def get_file_names(self):
        self.flight_intention_names=os.listdir(path_to_logs+"Flight_intentions")
        self.centralised_log_names=os.listdir(path_to_logs+"Centralised")
        self.decentralised_log_names=os.listdir(path_to_logs+"Decentralised")
        self.hybrid_log_names=os.listdir(path_to_logs+"Hybrid")
        


    ##LOSLOG dataframe
    def create_loslog_dataframe(self):
        
        dataframe_cnt=0
        col_list = ["LOS_id", "Scenario_name", "LOS_exit_time", "LOS_start_time", "LOS_duration_time", "LAT1", "LON1", "ALT1", "LAT2", "LON2", "ALT2", "DIST","crash"]
        
        df = pd.DataFrame([col_list], columns=col_list)
        loslog_data_frame =self.spark.createDataFrame(df)
        
        los_id=0
        
        ##Read LOSLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
            log_type = file_name.split("_")[0]
            if log_type=="LOSLOG":
                if ii<len(self.centralised_log_names):
                    log_file=path_to_logs+"Centralised/"+file_name
                    concept="1" ##CENTRALISED
                elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                    log_file=path_to_logs+"Decentralised/"+file_name
                    concept="3" ##DECENTRALISED
                else:
                    log_file=path_to_logs+"Hybrid/"+file_name
                    concept="2" ##HYBRID              
                scenario_var = file_name.split("_")
                if scenario_var[3]=="very": 
                    density="very_low"
                    distribution=scenario_var[5]
                    repetition=scenario_var[6]
                    uncertainty=scenario_var[7]
                else:
                    density=scenario_var[3]
                    distribution=scenario_var[4]
                    repetition=scenario_var[5]
                    uncertainty=scenario_var[6]   
                if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty

                loslog_file = open(log_file, "r")
        
                loslog_list = list()
                cnt = 0
                for line in loslog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [los_id, scenario_name]
                    los_id=los_id+1
                    for iv, value in enumerate(line_list):
                        if iv<2:
                            tmp_list.append(float(value))
                        elif iv==2:
                            tmp_list.append(float(line_list[0])-float(line_list[1]))
                        elif iv>4 and iv <11:
                            tmp_list.append(float(value))
                        elif iv==11:
                            tmp_list.append(float(value[:-2]))
        
        
                    crash=False
                    #If the aircarft are closer than 1.7 in the horizontal diraction and 0.75 in the vetrical teh LOS is considere a crash
                    #Aircraft dimmensions from https://www.dji.com/gr/matrice600-pro/info#specs
                    if float(line_list[11][:-2])<=1.7 and abs(float(line_list[7])-float(line_list[10]))<0.75:
                        crash=True
                    tmp_list.append(crash)
                    loslog_list.append(tmp_list)
        
        
                df = pd.DataFrame(loslog_list, columns=col_list)
                if dataframe_cnt==0:
                    loslog_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    loslog_data_frame_tmp = self.spark.createDataFrame(df)
                    loslog_data_frame=loslog_data_frame.union(loslog_data_frame_tmp)
                
                

        #loslog_data_frame.show()
        return loslog_data_frame

    ####

    ##CONFLOG dataframe
    def create_conflog_dataframe(self):

        dataframe_cnt=0
        col_list = ["CONF_id", "Scenario_name", "CONF_detected_time", "CPALAT", "CPALON"] 
        df = pd.DataFrame([col_list], columns=col_list)
        conflog_data_frame =self.spark.createDataFrame(df) 
        
        conf_id=0
        
        ##Read CONFLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
            log_type = file_name.split("_")[0]
            if log_type=="CONFLOG":
                if ii<len(self.centralised_log_names):
                    log_file=path_to_logs+"Centralised/"+file_name
                    concept="1" ##CENTRALISED
                elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                    log_file=path_to_logs+"Decentralised/"+file_name
                    concept="3" ##DECENTRALISED
                else:
                    log_file=path_to_logs+"Hybrid/"+file_name
                    concept="2" ##HYBRID 
                scenario_var = file_name.split("_")

                if scenario_var[3]=="very": 
                    density="very_low"
                    distribution=scenario_var[5]
                    repetition=scenario_var[6]
                    uncertainty=scenario_var[7]
                else:
                    density=scenario_var[3]
                    distribution=scenario_var[4]
                    repetition=scenario_var[5]
                    uncertainty=scenario_var[6]  
                if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""    
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty
        
                conflog_file = open(log_file, "r")
        
                conflog_list = list()
                cnt = 0
                for line in conflog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [conf_id, scenario_name]
                    conf_id=conf_id+1
                    for iv, value in enumerate(line_list):
                        if iv==0 or iv==9:
                            tmp_list.append(float(value))
                        elif iv==10:
                            tmp_list.append(float(value[:-2]))
        
                    conflog_list.append(tmp_list)
        
        
                df = pd.DataFrame(conflog_list, columns=col_list)
                if dataframe_cnt==0:
                    conflog_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    conflog_data_frame_tmp = self.spark.createDataFrame(df)
                    conflog_data_frame=conflog_data_frame.union(conflog_data_frame_tmp)


       # conflog_data_frame.show()
        return conflog_data_frame

    ####

    ##GEOLOG dataframe
    def create_geolog_dataframe(self):
        
        dataframe_cnt=0
        col_list = ["GEO_id", "Scenario_name", "GEOF_NAME", "MAX_intrusion", "Violation_severity","Open_airspace"]
        df = pd.DataFrame([col_list], columns=col_list)
        geolog_data_frame =self.spark.createDataFrame(df)   
        geo_id=0          
        
        ##Read GEOLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
            log_type = file_name.split("_")[0]
            if log_type=="GEOLOG":
                if ii<len(self.centralised_log_names):
                    log_file=path_to_logs+"Centralised/"+file_name
                    concept="1" ##CENTRALISED
                elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                    log_file=path_to_logs+"Decentralised/"+file_name
                    concept="3" ##DECENTRALISED
                else:
                    log_file=path_to_logs+"Hybrid/"+file_name
                    concept="2" ##HYBRID 
                    
                scenario_var = file_name.split("_")
                if scenario_var[3]=="very": 
                    density="very_low"
                    distribution=scenario_var[5]
                    repetition=scenario_var[6]
                    uncertainty=scenario_var[7]
                else:
                    density=scenario_var[3]
                    distribution=scenario_var[4]
                    repetition=scenario_var[5]
                    uncertainty=scenario_var[6]   
                if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty        
        
                geolog_file = open(log_file, "r")
        
                geolog_list = list()
                cnt = 0
                for line in geolog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [geo_id,scenario_name]
                    geo_id=geo_id+1
                    for iv, value in enumerate(line_list):
                        if iv==3:
                            tmp_list.append(value)
                        elif iv==4:
                            tmp_list.append(float(value))
                            
                    severity=True
                    #If the violation is less than 1 meter it is not considered severe
                    if float(line_list[4])<1:
                        severity=False
                    tmp_list.append(severity)
                    
                    open_airspace=False
                    #If the geofence name starts with g it is in open airspace
                    if line_list[3][0]=="G":
                        open_airspace=True
                    tmp_list.append(open_airspace)
                    geolog_list.append(tmp_list)
        
        
        
                df = pd.DataFrame(geolog_list, columns=col_list)
                if dataframe_cnt==0:
                    geolog_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    geolog_data_frame_tmp = self.spark.createDataFrame(df)
                    geolog_data_frame=geolog_data_frame.union(geolog_data_frame_tmp)
        

        #geolog_data_frame.show()
        return geolog_data_frame

    ####
    ##FLSTLOG dataframe
    def create_flstlog_dataframe(self):
        
        col_list = ['Flight_id', "scenario_name", "ACID", "Origin_LAT","Origin_LON", "Dest_LAT","Dest_LON", "Baseline_deparure_time", "cruising_speed",
                    "Priority","loitering","Baseline_2D_distance","Baseline_vertical_distance","Baseline_ascending_distance","Baseline_3D_distance",\
                        "Baseline_flight_time","Baseline_arrival_time","Dest_x","Dest_y"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        flstlog_data_frame =self.spark.createDataFrame(df)   
        flst_id=0                    
                    
        for ii,file_name in enumerate(self.flight_intention_names):
            print(file_name)
            flight_file=path_to_logs+"Flight_intentions/"+file_name
            scenario_var = file_name.split("_")
            if scenario_var[2]=="very": 
                density="very_low"
                distribution=scenario_var[4]
                repetition=scenario_var[5]
                uncertainty=scenario_var[6].split(".")[0]   
            else:
                density=scenario_var[2]
                distribution=scenario_var[3]
                repetition=scenario_var[4]
                uncertainty=scenario_var[5].split(".")[0]  
            if uncertainty[0]!="R"and uncertainty[0]!="W":
                uncertainty=""                  
            #scenario_name2="_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty 
            
            scenario_name_centr="1_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty
            scenario_name_hybrid="2_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty
            scenario_name_decentr="3_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty

            flight_int_file = open(flight_file, "r")
            flint_list = list()
            for line in  flight_int_file:

                flight_data=line.split(",")
                acid=flight_data[1]
                aircraft_type=flight_data[2]
                time_data=flight_data[3].split(":")
                time=int(time_data[2])+int(time_data[1])*60+int(time_data[0])*3600
                priority=flight_data[8]
                origin_lon=flight_data[4].split("(")[1]
                dest_lon=flight_data[6].split("(")[1]
                origin_lat=flight_data[5].split(")")[0]
                dest_lat=flight_data[7].split(")")[0] 
                
                transformer = Transformer.from_crs('epsg:4326','epsg:32633')
                p=transformer.transform( dest_lat,dest_lon)
                dest_x=p[0]
                dest_y=p[1]
                
                loitering=False
                if flight_data[9]!="":
                    loitering=True
                    
                cruising_speed=10.29 # m/s
                if aircraft_type=="MP30": 
                    cruising_speed=15.43 # m/s
                    
                base_2d_dist=0#TODO : create a function to compute that with inputs origin_lat,origin_lon,dest_lat,dest_lon
                base_vertical_dist=0 #TODO : create a function to compute that, it should be 0 for all flights except loitering
                base_ascending_dist=0 #TODO : create a function to compute that, it should be 0 for all flights except loitering
                base_3d_dist=base_2d_dist+base_vertical_dist
                base_flight_time=0  #TODO : create a function to compute that with inputs base_2d_dist, base_vertical_dist and cruising_speed
                base_arrival_time=base_flight_time+time
                
                
                tmp_list = [flst_id,scenario_name_centr, acid,origin_lat,origin_lon,dest_lat,dest_lon,time,cruising_speed,priority,loitering,base_2d_dist,base_vertical_dist,base_ascending_dist,\
                            base_3d_dist,base_flight_time,base_arrival_time,dest_x,dest_y]
                flst_id=flst_id+1
                flint_list.append(tmp_list)
                tmp_list = [flst_id,scenario_name_hybrid, acid,origin_lat,origin_lon,dest_lat,dest_lon,time,cruising_speed,priority,loitering,base_2d_dist,base_vertical_dist,base_ascending_dist,\
                            base_3d_dist,base_flight_time,base_arrival_time,dest_x,dest_y]
                flst_id=flst_id+1
                flint_list.append(tmp_list)
                tmp_list = [flst_id,scenario_name_decentr, acid,origin_lat,origin_lon,dest_lat,dest_lon,time,cruising_speed,priority,loitering,base_2d_dist,base_vertical_dist,base_ascending_dist,\
                            base_3d_dist,base_flight_time,base_arrival_time,dest_x,dest_y]
                flst_id=flst_id+1
                flint_list.append(tmp_list)
            df = pd.DataFrame(flint_list, columns=col_list)

            if dataframe_cnt==0:
                flstlog_data_frame = self.spark.createDataFrame(df)
                dataframe_cnt=1
            else:
                flint_data_frame_tmp = self.spark.createDataFrame(df)
                flstlog_data_frame=flstlog_data_frame.union(flint_data_frame_tmp)   
        
        
        col_list = ["scenario_name2", "ACID2", "DEL_time", "SPAWN_time",
                     "FLIGHT_time", "2D_dist", "3D_dist", "ALT_dist",  "DEL_LAT" \
             , "DEL_LON", "DEL_ALT","Ascend_dist","work_done","DEL_x","DEL_y" ]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        flst_data_frame =self.spark.createDataFrame(df)  
 ##Read FLSTLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
             log_type = file_name.split("_")[0]
             if log_type=="FLSTLOG":
                 if ii<len(self.centralised_log_names):
                     log_file=path_to_logs+"Centralised/"+file_name
                     concept="1" ##CENTRALISED
                 elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                     log_file=path_to_logs+"Decentralised/"+file_name
                     concept="3" ##DECENTRALISED
                 else:
                     log_file=path_to_logs+"Hybrid/"+file_name
                     concept="2" ##HYBRID 
                     
                 scenario_var = file_name.split("_")
                 if scenario_var[3]=="very": 
                     density="very_low"
                     distribution=scenario_var[5]
                     repetition=scenario_var[6]
                     uncertainty=scenario_var[7]
                 else:
                     density=scenario_var[3]
                     distribution=scenario_var[4]
                     repetition=scenario_var[5]
                     uncertainty=scenario_var[6]    
                 if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""
                 scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty               
         
         
                 flstlog_file = open(log_file, "r")
         
                 flstlog_list = list()
                 cnt = 0
                 for line in flstlog_file:
                     cnt = cnt + 1
                     if cnt < 10:
                         continue
                     line_list = line.split(",")
                     
                     ascend_dist=0 #TODO : create a function to compute that with inputs ALT_dist=float(line_list[6]) and DEL_ALT=float(line_list[10])
                     work_done=0 #TODO : create a function to compute that with inputs ascend_dist and FLIGHT_time=float(line_list[3])
                     
                     transformer = Transformer.from_crs('epsg:4326','epsg:32633')
                     p=transformer.transform(  float(line_list[8]), float(line_list[9]))
                     del_x=p[0]
                     del_y=p[1]
                     
                     tmp_list = [scenario_name, line_list[1], float(line_list[0]), float(line_list[2]),float(line_list[3]),float(line_list[4]), float(line_list[5]), float(line_list[6]),\
                                 float(line_list[8]), float(line_list[9]), float(line_list[10]),ascend_dist,work_done,del_x,del_y]
         
                     flstlog_list.append(tmp_list)
         
         
                 df = pd.DataFrame(flstlog_list, columns=col_list)
 
                 if dataframe_cnt==0:
                     flst_data_frame = self.spark.createDataFrame(df)
                     dataframe_cnt=1
                 else:
                     flst_data_frame_tmp = self.spark.createDataFrame(df)
                     flst_data_frame=flst_data_frame.union(flst_data_frame_tmp)        
        
        flstlog_data_frame=flstlog_data_frame.join(flst_data_frame,(flstlog_data_frame["ACID"]==flst_data_frame["ACID2"] ) & (flst_data_frame["scenario_name2"]==flstlog_data_frame["scenario_name"]),"outer")
        flstlog_data_frame=flstlog_data_frame.drop(*("ACID2","scenario_name2"))  
        
        flstlog_data_frame = flstlog_data_frame.withColumn('Arrival_delay', flstlog_data_frame["DEL_time"] -flstlog_data_frame["Baseline_arrival_time"])
        flstlog_data_frame = flstlog_data_frame.withColumn('Departure_delay', flstlog_data_frame["SPAWN_time"] -flstlog_data_frame["Baseline_deparure_time"])
        flstlog_data_frame = flstlog_data_frame.withColumn('Spawned', when(flstlog_data_frame["SPAWN_time"].isNull(),False ).otherwise(True))
        flstlog_data_frame = flstlog_data_frame.withColumn('Mission_completed', when((flstlog_data_frame["Dest_x"]-flstlog_data_frame["Del_x"])*(flstlog_data_frame["Dest_x"]-flstlog_data_frame["Del_x"])+(flstlog_data_frame["Dest_y"]-flstlog_data_frame["Del_y"])*(flstlog_data_frame["Dest_y"]-flstlog_data_frame["Del_y"])>400,False ).otherwise(True))
        flstlog_data_frame=flstlog_data_frame.drop(*("Dest_x","Dest_y","Del_x","Del_y"))  
        
        #flstlog_data_frame.show()                 

        return flstlog_data_frame
    


    ####

    ##REGLOG dataframe
    def read_reglog(self, log_file):
        reglog_file = open(log_file, "r")

        acid_lines_list = []
        alt_lines_list = []
        lon_lines_list = []
        lat_lines_list = []

        cnt_modulo = 0
        cnt = 0
        for line in reglog_file:
            cnt = cnt + 1
            if cnt < 10:
                continue

            if cnt_modulo % 4 == 0:
                acid_lines_list.append(line[:-2])
            elif cnt_modulo % 4 == 1:
                alt_lines_list.append(line[:-2])
            elif cnt_modulo % 4 == 2:
                lat_lines_list.append(line[:-2])
            else:
                lon_lines_list.append(line[:-2])
            cnt_modulo = cnt_modulo + 1

        return acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list

    ####

    ##REGLOG dataframe
    def create_reglog_dataframe(self):
        
        col_list = ["REG_id", "scenario_name", "Time_stamp", "ACID", "ALT", "LAT", "LON"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        reglog_data_frame =self.spark.createDataFrame(df)
        reglog_object_counter = 0
        
        ##Read REGLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
            log_type = file_name.split("_")[0]
            if log_type=="REGLOG":
                if ii<len(self.centralised_log_names):
                    log_file=path_to_logs+"Centralised/"+file_name
                    concept="1" ##CENTRALISED
                elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                    log_file=path_to_logs+"Decentralised/"+file_name
                    concept="3" ##DECENTRALISED
                else:
                    log_file=path_to_logs+"Hybrid/"+file_name
                    concept="2" ##HYBRID              
                scenario_var = file_name.split("_")
                if scenario_var[3]=="very": 
                    density="very_low"
                    distribution=scenario_var[5]
                    repetition=scenario_var[6]
                    uncertainty=scenario_var[7]
                else:
                    density=scenario_var[3]
                    distribution=scenario_var[4]
                    repetition=scenario_var[5]
                    uncertainty=scenario_var[6]      
                if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty       
                
                
        
                acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = self.read_reglog(log_file)

                reglog_list = list()
                
        
                for i, line in enumerate(acid_lines_list):
                    acid_line_list = line.split(",")
                alt_line_list = alt_lines_list[i].split(",")
                lat_line_list = lat_lines_list[i].split(",")
                lon_line_list = lon_lines_list[i].split(",")
        
                for iv, value in enumerate(acid_line_list):
                    if iv == 0:
                        continue
        
                    tmp_list = [reglog_object_counter, scenario_name, float(acid_line_list[0])]
             
                    tmp_list.append(value)
                    tmp_list.append(alt_line_list[iv])
                    tmp_list.append(lat_line_list[iv])
                    tmp_list.append(lon_line_list[iv])
        
                    reglog_object_counter = reglog_object_counter + 1
        
                    reglog_list.append(tmp_list)  # pyspark gives an error when trying to pass list as a value

        

                df = pd.DataFrame(reglog_list, columns=col_list)
                if dataframe_cnt==0:
                    reglog_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    reglog_data_frame_tmp = self.spark.createDataFrame(df)
                    reglog_data_frame=reglog_data_frame.union(reglog_data_frame_tmp)
        # reglog_data_frame.show()

        return reglog_data_frame

    ##time object dataframe
    def create_time_object_dataframe(self):

        col_list = ["Time_object_id", "scenario_name", "Time_stamp", "#Aircaft_Alive", "Sound_exposure_p1",
                    "Sound_exposure_p2",
                    "Sound_exposure_p3"]  ## add as many "Sound_exposure_p1" as the points of interest
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        time_data_frame  =self.spark.createDataFrame(df)
        time_object_cnt = 0
        
        ##Read REGLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
            log_type = file_name.split("_")[0]
            if log_type=="REGLOG":
                if ii<len(self.centralised_log_names):
                    log_file=path_to_logs+"Centralised/"+file_name
                    concept="1" ##CENTRALISED
                elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                    log_file=path_to_logs+"Decentralised/"+file_name
                    concept="3" ##DECENTRALISED
                else:
                    log_file=path_to_logs+"Hybrid/"+file_name
                    concept="2" ##HYBRID              
                scenario_var = file_name.split("_")
                if scenario_var[3]=="very": 
                    density="very_low"
                    distribution=scenario_var[5]
                    repetition=scenario_var[6]
                    uncertainty=scenario_var[7]
                else:
                    density=scenario_var[3]
                    distribution=scenario_var[4]
                    repetition=scenario_var[5]
                    uncertainty=scenario_var[6]   
                if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty  
        

                acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = self.read_reglog(log_file)
        
                
                time_list = list()
        
                for line in acid_lines_list:
                    line_list = line.split(",")
                    
                    aircraft_counter = len(line_list) - 1
        
                    sound_exp_p1=0 #TODO : create a function to compute that with inputs the point p1, and the regolog_dataframe, same for teh rest of the points
                    sound_exp_p2=0
                    sound_exp_p3=0
                    #TODO : we need to define the points 
                    tmp_list = [time_object_cnt, scenario_name, float(line_list[0]),aircraft_counter,sound_exp_p1,sound_exp_p2,sound_exp_p3]
                    time_object_cnt = time_object_cnt + 1
                    time_list.append(tmp_list)



                df = pd.DataFrame(time_list, columns=col_list)
                if dataframe_cnt==0:
                    time_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    time_data_frame_tmp = self.spark.createDataFrame(df)
                    time_data_frame=time_data_frame.union(time_data_frame_tmp)
                    

        #time_data_frame.show()
        return time_data_frame

    ####

    ##metrics dataframe
    def create_metrics_dataframe(self):
        col_list = ["Scenario_name", "#Aircraft_number","#Succeful_aircraft_number","#Spawned_aircraft_number", "AEQ1", "AEQ1_1", "AEQ2", "AEQ2_1", "AEQ3", "AEQ4", "AEQ5",
                    "AEQ5_1", "CAP1", "CAP2", "ENV1", "ENV2", "ENV4", "SAF1", "SAF2", "SAF2_1", "SAF3",
                    "SAF4", "SAF5", "SAF6", "SAF6_1" \
                                    ,"EFF1", "EFF2", "EFF3", "EFF4", "EFF5", "EFF6",  "PRI1", "PRI2",
                    "PRI3", "PRI4", "PRI5"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        metrics_data_frame =self.spark.createDataFrame(df) 
        metrics_list = list()            
        
        ##Read GEOLOGs
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
            log_type = file_name.split("_")[0]
            if log_type=="GEOLOG":
                if ii<len(self.centralised_log_names):
                    log_file=path_to_logs+"Centralised/"+file_name
                    concept="1" ##CENTRALISED
                elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                    log_file=path_to_logs+"Decentralised/"+file_name
                    concept="3" ##DECENTRALISED
                else:
                    log_file=path_to_logs+"Hybrid/"+file_name
                    concept="2" ##HYBRID 
                    
                scenario_var = file_name.split("_")
                if scenario_var[3]=="very": 
                    density="very_low"
                    distribution=scenario_var[5]
                    repetition=scenario_var[6]
                    uncertainty=scenario_var[7]
                else:
                    density=scenario_var[3]
                    distribution=scenario_var[4]
                    repetition=scenario_var[5]
                    uncertainty=scenario_var[6]       
                if uncertainty[0]!="R"and uncertainty[0]!="W":
                    uncertainty=""
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty  
                
                #Filtered flstlog by scenario name
                
                filtered_flst_dataframe=self.flst_log_dataframe.filter(self.flst_log_dataframe["scenario_name"]==scenario_name) 
                filtered_reg_dataframe=self.reg_log_dataframe.filter(self.reg_log_dataframe["scenario_name"]==scenario_name) 
                filtered_geo_dataframe=self.geo_log_dataframe.filter(self.geo_log_dataframe["scenario_name"]==scenario_name) 
                filtered_los_dataframe=self.los_log_dataframe.filter(self.los_log_dataframe["scenario_name"]==scenario_name) 
                filtered_conf_dataframe=self.conf_log_dataframe.filter(self.conf_log_dataframe["scenario_name"]==scenario_name) 
                
        
                aircraft_number=0 #TODO : create a function to compute that with inputs the filtered_flst_dataframe, that value is the number of aircrafts from flight intentions
                aircraft_succesful_number=0 #TODO : create a function to compute that with inputs teh filtered_flst_dataframe
                aircraft_spawned_number=0 #TODO : create a function to compute that with inputs the filtered_flst_dataframe
                aeq1=0 #TODO : create a function to compute that with inputs the arrival_delay of the filtered_flst_dataframe
                aeq1_1=0 #TODO : create a function to compute that with inputs the aeq1 and the aircraft_number
                aeq2=0 #TODO : create a function to compute that with inputs the flight_time of the filtered_flst_dataframe
                aeq2_1=0#TODO : create a function to compute that with inputs the aeq2 and the aircraft_number
                aeq3=0 #TODO : create a function to compute that with inputs the arrival_delay of the filtered_flst_dataframe
                aeq4=0 #TODO : create a function to compute that with inputs the arrival_delay of the filtered_flst_dataframe
                aeq5=0 #TODO : create a function to compute that with inputs the arrival_delay of the filtered_flst_dataframe
                aeq5_1=0#TODO : create a function to compute that with inputs the aeq5 and the aircraft_number
                eff1=0 #TODO : create a function to compute that with inputs the Baseline_2D_distance and the 2D_dist of the filtered_flst_dataframe
                eff2=0 #TODO : create a function to compute that with inputs the Baseline_vertical_distance and the ALT_dist of the filtered_flst_dataframe
                eff3=0 #TODO : create a function to compute that with inputs the Baseline_acending_distance and the Ascending_dist of the filtered_flst_dataframe
                eff4=0 #TODO : create a function to compute that with inputs the Baseline_3D_distance and the 3D_dist of the filtered_flst_dataframe
                eff5=0 #TODO : create a function to compute that with inputs the Baseline_flight_time and the FLIGHT_time of the filtered_flst_dataframe
                eff6=0 #TODO : create a function to compute that with inputs the Departure_delay of the filtered_flst_dataframe
                pri1=0 #TODO : create a function to compute that with inputs the FLIGHT_time and priority of the filtered_flst_dataframe
                pri2=0 #TODO : create a function to compute that with inputs the 3D_dist and priority of the filtered_flst_dataframe
                pri3=0 #TODO : create a function to compute that with inputs the FLIGHT_time and priority of the filtered_flst_dataframe
                pri4=0 #TODO : create a function to compute that with inputs the 3D_dist and priority of the filtered_flst_dataframe
                pri5=0 #TODO : create a function to compute that with inputs the arrival_delay and priority of the filtered_flst_dataframe
                saf1=0 #TODO : create a function to compute that with input the filtered_conf_dataframe
                saf2=0 #TODO : create a function to compute that with input the filtered_los_dataframe
                saf2_1=0 #TODO : create a function to compute that with input the crashes of the filtered_los_dataframe
                saf3=0 #TODO : create a function to compute that with input saf1,saf2
                saf4=0 #TODO : create a function to compute that with input the DIST of the filtered_los_dataframe
                saf5=0 #TODO : create a function to compute that with input the LOS_duration_time of the filtered_los_dataframe
                saf6=0 #TODO : create a function to compute that with input the filtered_geo_dataframe
                saf6_1=0 #TODO : create a function to compute that with input the violation_severity of the  filtered_geo_dataframe
                
                cap1=0 #TODO : create a function to compute that with inputs the arrival_delay of the filtered_flst_dataframe
                cap2=0 #TODO : create a function to compute that with inputs the saf2 and aircraft_number
                env1=0 #TODO : create a function to compute that with input the workd_done of the  filtered_flst_dataframe
                env2=0 #TODO : create a function to compute that with input the filtered_reg_dataframe
                env4=0 #TODO : create a function to compute that with input the filtered_reg_dataframe
                

                tmp_list = [scenario_name, aircraft_number,aircraft_succesful_number,aircraft_spawned_number, aeq1,aeq1_1, aeq2, aeq2_1, aeq3,aeq4,aeq5,
                            aeq5_1,cap1,cap2,env1,env2,env4,saf1,saf2,saf2_1,saf3,saf4,saf5,saf6,saf6_1,eff1,eff2,eff3,eff4,eff5,eff6,pri1,pri2,pri3,pri4,pri5]
                    
                
                metrics_list.append(tmp_list)
        
        df = pd.DataFrame(metrics_list, columns=col_list)
        metrics_data_frame = self.spark.createDataFrame(df)

                    
                    
        col_list = ["Scenario_name2","CAP3", "CAP4"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        metrics_list = list()
        cap_data_frame =self.spark.createDataFrame(df)   
        for ii,file_name in enumerate(self.centralised_log_names+self.decentralised_log_names+self.hybrid_log_names):
           log_type = file_name.split("_")[0]
           if log_type=="GEOLOG":
               if ii<len(self.centralised_log_names):
                   log_file=path_to_logs+"Centralised/"+file_name
                   concept="1" ##CENTRALISED
               elif ii<len(self.centralised_log_names)+len(self.decentralised_log_names):
                   log_file=path_to_logs+"Decentralised/"+file_name
                   concept="3" ##DECENTRALISED
               else:
                   log_file=path_to_logs+"Hybrid/"+file_name
                   concept="2" ##HYBRID 
                   
               scenario_var = file_name.split("_")
               if scenario_var[3]=="very": 
                   density="very_low"
                   distribution=scenario_var[5]
                   repetition=scenario_var[6]
                   uncertainty=scenario_var[7]
               else:
                   density=scenario_var[3]
                   distribution=scenario_var[4]
                   repetition=scenario_var[5]
                   uncertainty=scenario_var[6]       
               if uncertainty[0]!="R":
                   continue
               scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty  
               deterministic_scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+""

               cap1_rogue=metrics_data_frame.filter(metrics_data_frame["Scenario_name"]==scenario_name).select("CAP1").collect()[0][0]
               cap1_determ=0#metrics_data_frame.filter(metrics_data_frame["Scenario_name"]==deterministic_scenario_name).select("CAP1").collect()[0] #TODO :uncommnet that for the real data
               cap2_rogue=metrics_data_frame.filter(metrics_data_frame["Scenario_name"]==scenario_name).select("CAP2").collect()[0][0 ]
               cap2_determ=0#metrics_data_frame.filter(metrics_data_frame["Scenario_name"]==deterministic_scenario_name).select("CAP2").collect()[0] #TODO :uncommnet that for the real data
               cap3=cap1_rogue-cap1_determ
               cap4=cap2_rogue-cap2_determ
               tmp_list = [scenario_name, cap3,cap4]
                    
                
               metrics_list.append(tmp_list)
        
        df = pd.DataFrame(metrics_list, columns=col_list)
        cap_data_frame = self.spark.createDataFrame(df)
    

        metrics_data_frame=metrics_data_frame.join(cap_data_frame, (cap_data_frame["Scenario_name2"]==metrics_data_frame["Scenario_name"]),"outer")
        metrics_data_frame=metrics_data_frame.drop("Scenario_name2")  
        
        metrics_data_frame.show()
        return metrics_data_frame

    ####

    ##senario (scn file) dataframe
    def create_fp_intention_dataframe(self, filePath):
        schema = StructType([
            StructField("RECEPTION_TIME_FP_INDEX", StringType(), True),
            StructField("FPLAN_ID_INDEX", StringType(), True),
            StructField("VEHICLE_INDEX", StringType(), True),
            StructField("DEPARTURE_INDEX", StringType(), True),
            StructField("INITIAL_LOCATION_INDEX", StringType(), True),
            StructField("FINAL_LOCATION_INDEX", StringType(), True),
            StructField("PRIORITY_INDEX", StringType(), True),
            StructField("STATUS_INDEX", StringType(), True),
            StructField("GEOFENCE_DURATION", StringType(), True),
            StructField("GEOFENCE_BBOX_POINT1", StringType(), True),
            StructField("GEOFENCE_BBOX_POINT2", StringType(), True)
        ])

        fp_intention_dataframe = self.spark.read.csv(filePath, header=False, schema=schema)
        #fp_intention_dataframe.show()
        return fp_intention_dataframe