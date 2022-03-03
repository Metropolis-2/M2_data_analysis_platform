# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 17:42:34 2022

@author: jpedrero
"""
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when
import os

path_to_logs="example_logs/"

class DataframeCreator():

    def __init__(self, scenario_name, spark):
        #self.scenario_name = scenario_name
        self.spark = spark
        self.centralised_log_names=[]
        self.decentralised_log_names=[]
        self.hybrid_log_names=[]
        self.flight_intention_names=[]
        self.get_file_names()
        return
    
    def get_file_names(self):
        self.flight_intention_names=os.listdir(path_to_logs+"Flight_intentions")
        self.centralised_log_names=os.listdir(path_to_logs+"Centralised")
        self.decentralised_log_names=os.listdir(path_to_logs+"Decentralised")
        self.hybrid_log_names=os.listdir(path_to_logs+"Hybrid")
        


    ##LOSLOG dataframe
    def create_loslog_dataframe(self):
        
        dataframe_cnt=0
        col_list = ["LOS_id", "Scenario_name", "LOS_exit_time", "LOS_start_time", "Time_of_min_distance", "ACID1", "ACID2", "LAT1", "LON1", "ALT1", "LAT2", "LON2", "ALT2", "DIST"]
        
        df = pd.DataFrame([col_list], columns=col_list)
        loslog_data_frame =self.spark.createDataFrame(df)
        
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty

                loslog_file = open(log_file, "r")
        
                loslog_list = list()
                cnt = 0
                for line in loslog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [cnt - 10, scenario_name]
                    for iv, value in enumerate(line_list):
                        if iv < 3 or (iv > 4 and iv < 11):
                            tmp_list.append(float(value))
                        elif iv == 11:
                            tmp_list.append(float(value[:-2]))
                        else:
                            tmp_list.append(value)
        
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
        col_list = ["CONF_id", "Scenario_name", "CONF_detected_time", "ACID1", "ACID2", "LAT1", "LON1", "ALT1", "LAT2","LON2", "ALT2", "CPALAT", "CPALON"] 
        df = pd.DataFrame([col_list], columns=col_list)
        conflog_data_frame =self.spark.createDataFrame(df)        
        
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty
        
                conflog_file = open(log_file, "r")
        
                conflog_list = list()
                cnt = 0
                for line in conflog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [cnt - 10, scenario_name]
                    for iv, value in enumerate(line_list):
                        if (iv == 0 or iv > 2) and iv != 7:
                            tmp_list.append(float(value))
                        elif iv == 10:
                            tmp_list.append(float(value[:-2]))
                        else:
                            tmp_list.append(value)
        
                    conflog_list.append(tmp_list)
        

        
                df = pd.DataFrame(conflog_list, columns=col_list)
                if dataframe_cnt==0:
                    conflog_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    conflog_data_frame_tmp = self.spark.createDataFrame(df)
                    conflog_data_frame=conflog_data_frame.union(conflog_data_frame_tmp)


        #conflog_data_frame.show()
        return conflog_data_frame

    ####

    ##GEOLOG dataframe
    def create_geolog_dataframe(self):
        
        dataframe_cnt=0
        col_list = ["GEO_id", "Scenario_name", "Deletion_time", "ACID", "GEOF_ID", "GEOF_NAME", "MAX_intrusion",
                    "LAT_intrusion", "LON_intrusion", "intrusion_time"]
        df = pd.DataFrame([col_list], columns=col_list)
        geolog_data_frame =self.spark.createDataFrame(df)             
        
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty        
        
                geolog_file = open(log_file, "r")
        
                geolog_list = list()
                cnt = 0
                for line in geolog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [cnt - 10,scenario_name]
                    for iv, value in enumerate(line_list):
                        if (iv == 0 or iv > 3) and iv != 7:
                            tmp_list.append(float(value))
                        elif iv == 7:
                            tmp_list.append(float(value[:-2]))
                        else:
                            tmp_list.append(value)
        
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
        col_list = ['Flight_id', "scenario_name", "ACID", "Baseline_2d_path_length", "Baseline_3d_path_length", \
                    "Baseline_vertical_path_length", "Baseline_flight_duration", "DEL_time", "SPAWN_time",
                    "FLIGHT_time", "2D_dist", "3D_dist", "ALT_dist", "Work_done", "DEL_LAT" \
            , "DEL_LON", "DEL_ALT", "TAS", "DEL_VS", "DEL_HDG", "ASAS_active", "PILOT_ALT", "PILOT_SPD", "PILOT_HDG","PILOT_VS", "scenario_name2"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        flstlog_data_frame =self.spark.createDataFrame(df)    
         
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty               
        
        
                flstlog_file = open(log_file, "r")
        
                flstlog_list = list()
                cnt = 0
                for line in flstlog_file:
                    cnt = cnt + 1
                    if cnt < 10:
                        continue
                    line_list = line.split(",")
                    tmp_list = [cnt - 10, scenario_name, line_list[1], "-", "-", "-", "-"]
                    for iv, value in enumerate(line_list):
                        if iv == 0 or (iv > 1 and iv < 14) or (iv > 14 and iv < 18):
                            tmp_list.append(float(value))
                        if iv == 14:
                            tmp_list.append(value)
                        elif iv == 18:
                            tmp_list.append(float(value[:-2]))
                            
                    tmp_list.append(scenario_name[1:])
        
                    flstlog_list.append(tmp_list)
        
        
        
                df = pd.DataFrame(flstlog_list, columns=col_list)

                if dataframe_cnt==0:
                    flstlog_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    flstlog_data_frame_tmp = self.spark.createDataFrame(df)
                    flstlog_data_frame=flstlog_data_frame.union(flstlog_data_frame_tmp)
                    
        col_list = ["scenario_name2", "ACID2", "Origin_LAT","Origin_LON", "Dest_LAT","Dest_LON", "Baseline_deparure_time", "Aircarft_type",
                    "Priority"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        flint_data_frame =self.spark.createDataFrame(df)                       
                    
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
            scenario_name="_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty  

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
                
                tmp_list = [scenario_name, acid,origin_lat,origin_lon,dest_lat,dest_lon,time,aircraft_type,priority]
                flint_list.append(tmp_list)
            df = pd.DataFrame(flint_list, columns=col_list)

            if dataframe_cnt==0:
                flint_data_frame = self.spark.createDataFrame(df)
                dataframe_cnt=1
            else:
                flint_data_frame_tmp = self.spark.createDataFrame(df)
                flint_data_frame=flint_data_frame.union(flint_data_frame_tmp)            
            
            
      
        flstlog_data_frame=flstlog_data_frame.join(flint_data_frame,(flstlog_data_frame["ACID"]==flint_data_frame["ACID2"] ) & (flint_data_frame["scenario_name2"]==flstlog_data_frame["scenario_name2"]),"outer")
        flstlog_data_frame=flstlog_data_frame.drop(*("ACID2","scenario_name2"))
 
        #flint_data_frame.show()
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty       
                
                
        
                acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = self.read_reglog(log_file)

                reglog_list = list()
                reglog_object_counter = 0
        
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty  
        

                acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = self.read_reglog(log_file)
        
                time_object_cnt = 0
                time_list = list()
        
                for line in acid_lines_list:
                    line_list = line.split(",")
                    tmp_list = [time_object_cnt, scenario_name, float(line_list[0])]
                    time_object_cnt = time_object_cnt + 1
                    aircraft_counter = len(line_list) - 1
        
                    tmp_list.append(aircraft_counter)
                    tmp_list.append("-")
                    tmp_list.append("-")
                    tmp_list.append("-")
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
        col_list = ["Scenario_name", "#Aircraft_number", "AEQ1", "AEQ1_1", "AEQ2", "AEQ2_1", "AEQ3", "AEQ4", "AEQ5",
                    "AEQ5_1", "CAP1", "CAP2", "CAP3", "CAP4", "ENV1", "ENV2", "ENV3", "ENV4", "SAF1", "SAF2", "SAF3",
                    "SAF4", "SAF5", "SAF6" \
                                    "EFF1", "EFF2", "EFF3", "EFF4", "EFF5", "EFF6", "EFF7", "EFF8", "PRI1", "PRI2",
                    "PRI3", "PRI4", "PRI5"]
        dataframe_cnt=0
        df = pd.DataFrame([col_list], columns=col_list)
        metrics_data_frame =self.spark.createDataFrame(df)             
        
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
                scenario_name=concept+"_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty                
            
                metrics_list = list()
                tmp_list = [scenario_name, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
                            "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]
                metrics_list.append(tmp_list)
        
                df = pd.DataFrame(metrics_list, columns=col_list)
    
                if dataframe_cnt==0:
                    metrics_data_frame = self.spark.createDataFrame(df)
                    dataframe_cnt=1
                else:
                    metrics_data_frame_tmp = self.spark.createDataFrame(df)
                    metrics_data_frame=metrics_data_frame.union(metrics_data_frame_tmp)        



        #metrics_data_frame.show()
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