# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 17:42:34 2022
@author: jpedrero
"""
import pandas as pd
from pyproj import  Transformer
import os
import AEQ_metrics
import CAP_metrics
import EFF_metrics
import ENV_metrics
import SAF_metrics
import PRI_metrics 
import util_functions

import dill

def calc_flst_spawn_col(row):
    if type(row["SPAWN_time"])!=float or row["SPAWN_time"]>5400:
        return False
    else:
        return True
    
def calc_flst_mission_completed_col(row):
    if (row["Dest_x"]-row["DEL_x"])*(row["Dest_x"]-row["DEL_x"])+(row["Dest_y"]-row["DEL_y"])*(row["DEL_y"]-row ["DEL_y"])>400 or row["SPAWN_time"]>5400 or row["DEL_time"]>5400 or row["Spawned"] :
        return False
    else:
        return True

path_to_logs="input_logs/"

class DataframeCreator():

    def __init__(self):


        self.centralised_log_names=[]
        self.decentralised_log_names=[]
        self.hybrid_log_names=[]
        self.flight_intention_names=[]
        self.get_file_names()
        
        input_file=open("data/baseline_routes.dill", 'rb')
        self.baseline_length_dict=dill.load(input_file)
        
        ##The datframes  generation function shod be called in order,
        #as soem of teh em require already created dataframes
        #the order is self.create_flstlog_dataframe() 
        # self.create_loslog_dataframe() 
        #self.create_conflog_dataframe() 
        #self.create_geolog_dataframe()
        #self.create_env_metrics_dataframe()
        #self.create_density_dataframe()
        #self.create_metrics_dataframe()

        
        self.flst_log_dataframe,self.loitering_nfz_data_frame = self.create_flstlog_dataframe() 
        #self.los_log_dataframe = self.create_loslog_dataframe() 
        #self.conf_log_dataframe = self.create_conflog_dataframe() 
        #self.geo_log_dataframe = self.create_geolog_dataframe()
        
        #self.reg_log_dataframe = self.create_reglog_dataframe()
        #self.env_metrics_dataframe=self.create_env_metrics_dataframe()
        #self.dens_log_dataframe=self.create_density_dataframe()
        #self.time_log_dataframe=self.create_time_object_dataframe() 
        #self.metrics_dataframe=self.create_metrics_dataframe()
        return
    
    def get_file_names(self):
        self.flight_intention_names=os.listdir(path_to_logs+"Flight_intentions")
        self.centralised_log_names=os.listdir(path_to_logs+"Centralised")
        self.decentralised_log_names=os.listdir(path_to_logs+"Decentralised")
        self.hybrid_log_names=os.listdir(path_to_logs+"Hybrid")
        


    ##LOSLOG dataframe
    def create_loslog_dataframe(self):
        
        dataframe_cnt=0
        col_list = ["LOS_id", "Scenario_name", "LOS_exit_time", "LOS_start_time", "LOS_duration_time", "LAT1", "LON1", "ALT1", "LAT2", "LON2", "ALT2", "DIST","crash","in_time"]
        
        loslog_list = list()
        
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
                    if float(line_list[11][:-2])<=1.7 and abs(float(line_list[7])-float(line_list[10]))<2.46:
                        crash=True
                        
                    tmp_list.append(crash)
                    
                    in_time=False
                    if tmp_list[3]<=5400:
                        in_time=True
                    
                    tmp_list.append(in_time)
                    loslog_list.append(tmp_list)
        
        
    

        loslog_data_frame = pd.DataFrame(loslog_list, columns=col_list)

                
        print("LOSLOG dataframe created!")
        
        
        output_file=open("dills/loslog_dataframe.dill", 'wb')
        dill.dump(loslog_data_frame,output_file)
        output_file.close()
        

        return loslog_data_frame

    ####

    ##CONFLOG dataframe
    def create_conflog_dataframe(self):

        dataframe_cnt=0
        col_list = ["CONF_id", "Scenario_name", "CONF_detected_time", "CPALAT", "CPALON","in_time"] 

        conf_id=0
        conflog_list = list()
        
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
                            
                    in_time=False
                    if tmp_list[2]<=5400:
                        in_time=True
                    
                    tmp_list.append(in_time)
        
                    conflog_list.append(tmp_list)
        

       
        conflog_data_frame = pd.DataFrame(conflog_list, columns=col_list)

                
        print("CONFLOG Dataframe created!")
        
        
        output_file=open("dills/conflog_dataframe.dill", 'wb')
        dill.dump(conflog_data_frame,output_file)
        output_file.close()
        
        
        return conflog_data_frame

    ####

    ##GEOLOG dataframe
    def create_geolog_dataframe(self):
        
        input_file=open("dills/loitering_nfz_dataframe.dill", 'wb')
        loitering_nfz_data_frame=dill.load(input_file)
        input_file.close()
        
        input_file=open("dills/flstlog_dataframe.dill", 'wb')
        flstlog_data_frame=dill.load(input_file)
        input_file.close()
        #drop unused columns
        flstlog_data_frame=flstlog_data_frame.drop(["Baseline_deparure_time", "cruising_speed",
                    "Priority","loitering","Baseline_2D_distance","Baseline_vertical_distance","Baseline_ascending_distance","Baseline_3D_distance",\
                        "Baseline_flight_time","Baseline_arrival_time","DEL_time", "SPAWN_time",
                     "FLIGHT_time", "2D_dist", "3D_dist", "ALT_dist",  "DEL_LAT" \
             , "DEL_LON", "DEL_ALT","Ascend_dist","work_done"],axis=1)  
        
        dataframe_cnt=0
        col_list = ["GEO_id", "Scenario_name", "GEOF_NAME", "MAX_intrusion","Intrusion_time", "Violation_severity","Open_airspace","Loitering_nfz","Node_in_nfz","In_nfz_applied","in_time"]

        geo_id=0    
        
        geolog_list = list()
        
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
                        elif iv==7:
                            tmp_list.append(float(value[:-2]))
                            
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
                    loitering=False
                    #If the geofence name starts with l it is in open airspace
                    if line_list[3][0]=="L":
                        loitering=True
                    tmp_list.append(loitering)
                    
                    in_time=False
                    if tmp_list[4]<=5400:
                        in_time=True
                        
                    node_in_nfz=False
                    in_nfz_applied=False
                    if loitering:
                        loiter_filtered=loitering_nfz_data_frame[(loitering_nfz_data_frame["Scenario_name"]==scenario_name)&(loitering_nfz_data_frame["NFZ_name"]==line_list[3])]
                        nfz_applied_time=loiter_filtered["Applied_time"].values[0]
                        nfz_area=loiter_filtered["NFZ_area"].values[0]
                        flstlog_data_frame_filtered=flstlog_data_frame[(flstlog_data_frame["Scenario_name"]==scenario_name)&(flstlog_data_frame["ACID"]==line_list[1])]
                        
                        in_nfz_applied=util_functions.is_in_area_when_applied(nfz_applied_time,float(value[:-2]))
                        node_in_nfz=util_functions.has_orig_dest_in_nfz(flstlog_data_frame_filtered,nfz_area)
                        
                        
                    tmp_list.append(node_in_nfz)
                    tmp_list.append(in_nfz_applied)
                    
                    tmp_list.append(in_time)
                    
                    geolog_list.append(tmp_list)
        
        
        
        
        geolog_data_frame = pd.DataFrame(geolog_list, columns=col_list)

                
        print("GEOLOG Dataframe created!")
        
        
        output_file=open("dills/geolog_dataframe.dill", 'wb')
        dill.dump(geolog_data_frame,output_file)
        output_file.close()
        
        
        return geolog_data_frame

    ####
    
    
    ##FLSTLOG dataframe
    def create_flstlog_dataframe(self):
        
        
        col_list = ['Flight_id', "scenario_name", "ACID", "Origin_LAT","Origin_LON", "Dest_LAT","Dest_LON", "Baseline_deparure_time", "cruising_speed",
                    "Priority","loitering","Baseline_2D_distance","Baseline_vertical_distance","Baseline_ascending_distance","Baseline_3D_distance",\
                        "Baseline_flight_time","Baseline_arrival_time","Dest_x","Dest_y"]
            
        loitering_nfz_col_list=["Scenario_name","ACID","NFZ_name","NFZ_area"]
        nfz_list = list()
        
        flint_list = list()
        
        flst_id=0               
        
        uncertainties_list=["","R1","R2","R3","W1","W3","W5"]
                    
        for ii,file_name in enumerate(self.flight_intention_names):
            print(file_name)
            
            flight_file=path_to_logs+"Flight_intentions/"+file_name
            file_name=file_name.split(".")[0]
            scenario_var = file_name.split("_")
            if scenario_var[2]=="very": 
                density="very_low"
                distribution=scenario_var[4]
                repetition=scenario_var[5]

     
            else:
                density=scenario_var[2]
                distribution=scenario_var[3]
                repetition=scenario_var[4]

            if distribution=="40":
                uncertainties=uncertainties_list
            else:
                uncertainties=[""]
                
            for uncertainty in uncertainties:

            
                scenario_name_centr="1_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty
                scenario_name_hybrid="2_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty
                scenario_name_decentr="3_"+density+"_"+distribution+"_"+repetition+"_"+uncertainty

       
    
                flight_int_file = open(flight_file, "r")
                
                for line in  flight_int_file:
    
                    flight_data=line.split(",")
                    acid=flight_data[1]
                    aircraft_type=flight_data[2]
                    time_data=flight_data[3].split(":")
                    time=int(time_data[2])+int(time_data[1])*60+int(time_data[0])*3600
                    priority=flight_data[8]
                    origin_lon=round(float(flight_data[4].split("(")[1]),10)
                    dest_lon=round(float(flight_data[6].split("(")[1]),10)
                    origin_lat=round(float(flight_data[5].split(")")[0]),10)
                    dest_lat=round(float(flight_data[7].split(")")[0]) ,10)
                    
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
                        
                    base_2d_dist=self.baseline_length_dict[str(origin_lat)+"-"+str(origin_lon)+"-"+str(dest_lat)+"-"+str(dest_lon)]
                    base_vertical_dist=util_functions.compute_baseline_vertical_distance(loitering)
                    base_ascending_dist=util_functions.compute_baseline_ascending_distance()
                    base_3d_dist=base_2d_dist+base_vertical_dist
                    base_flight_time=base_2d_dist/cruising_speed
                    base_arrival_time=base_flight_time+time
                
                    if flight_data[9]!="":
                        area_str=flight_data[10]+"-"+flight_data[11]+"-"+flight_data[12]+"-"+flight_data[13]
                        nfz_name="LOITER"+acid
                        print(nfz_name)
                        nfz_list.append([scenario_name_centr,acid,nfz_name,area_str])
                        nfz_list.append([scenario_name_hybrid,acid,nfz_name,area_str])
                        nfz_list.append([scenario_name_decentr,acid,nfz_name,area_str])
                    
                    
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
        flstlog_data_frame = pd.DataFrame(flint_list, columns=col_list)
        
        loitering_nfz_data_frame = pd.DataFrame(nfz_list, columns=loitering_nfz_col_list)
  
        
        
        col_list = ["scenario_name", "ACID", "DEL_time", "SPAWN_time",
                     "FLIGHT_time", "2D_dist", "3D_dist", "ALT_dist",  "DEL_LAT" \
             , "DEL_LON", "DEL_ALT","Ascend_dist","work_done","DEL_x","DEL_y" ]
        dataframe_cnt=0
        flstlog_list = list()
       
        loitering_nfz_col_list=["Scenario_name","ACID","Applied_time"]
        nfz_list = list()
        
        
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
         
                 
                 cnt = 0
                 for line in flstlog_file:
                     cnt = cnt + 1
                     if cnt < 10:
                         continue
                     line_list = line.split(",")
                     
                     ascend_dist=util_functions.compute_ascending_distance(float(line_list[6]),float(line_list[10]))
                     work_done=util_functions.compute_work_done(ascend_dist,float(line_list[3])) 
                     
                     transformer = Transformer.from_crs('epsg:4326','epsg:32633')
                     p=transformer.transform(  float(line_list[8]), float(line_list[9]))
                     del_x=p[0]
                     del_y=p[1]
                     
                     tmp_list = [scenario_name, line_list[1], float(line_list[0]), float(line_list[2]),float(line_list[3]),float(line_list[4]), float(line_list[5]), float(line_list[6]),\
                                 float(line_list[8]), float(line_list[9]), float(line_list[10]),ascend_dist,work_done,del_x,del_y]
         
                     flstlog_list.append(tmp_list)
                     nfz_list.append([scenario_name,line_list[1],float(line_list[0])])
         
         
        flst_data_frame = pd.DataFrame(flstlog_list, columns=col_list)
        nfz_flst_data_frame = pd.DataFrame(nfz_list, columns=loitering_nfz_col_list)


        flstlog_data_frame=pd.merge(flst_data_frame,flstlog_data_frame,on=["ACID","scenario_name"],how="outer")
        flstlog_data_frame['Arrival_delay']  =   flstlog_data_frame["DEL_time"] -flstlog_data_frame["Baseline_arrival_time"]
        flstlog_data_frame['Departure_delay']=flstlog_data_frame["SPAWN_time"] -flstlog_data_frame["Baseline_deparure_time"]
        flstlog_data_frame['Spawned'] =flstlog_data_frame.apply(calc_flst_spawn_col,axis=1)
        flstlog_data_frame['Mission_completed'] =flstlog_data_frame.apply(calc_flst_mission_completed_col,axis=1)
        
        
        flstlog_data_frame=flstlog_data_frame.drop(["Dest_x","Dest_y","DEL_x","DEL_y"],axis=1)  
        

        loitering_nfz_data_frame=pd.merge(loitering_nfz_data_frame,nfz_flst_data_frame,on=["Scenario_name","ACID"],how="inner")


        print("FLSTLOG Dataframe created!")
        

        
        
        output_file=open("dills/flstlog_dataframe.dill", 'wb')
        dill.dump(flstlog_data_frame,output_file)
        output_file.close()
          
        output_file=open("dills/loitering_nfz_dataframe.dill", 'wb')
        dill.dump(loitering_nfz_data_frame,output_file)
        output_file.close()
        return flstlog_data_frame,loitering_nfz_data_frame
    

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
    def create_env_metrics_dataframe(self):
        
        col_list = ["Scenario_name","ENV2","ENV4"]
        
        env_mertics_list = list()
        
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
                print(scenario_name)
                
                
        
                acid_lines_list, alt_lines_list, lon_lines_list, lat_lines_list = self.read_reglog(log_file)
                acid_reg_dict={}

            
        
                for i, line in enumerate(acid_lines_list):
                    acid_line_list = line.split(",")
                    if float(acid_line_list[0])>5400:
                        break
                    

                    alt_line_list = alt_lines_list[i].split(",")
                    lat_line_list = lat_lines_list[i].split(",")
                    lon_line_list = lon_lines_list[i].split(",")
                    
                    ##The next lines where in the outer intention
                    if i>len(lon_lines_list)-1:
                        print('Problem with',file_name)
                        continue
            
                    for iv, value in enumerate(acid_line_list):
                        if iv == 0:
                            continue
                        if value in acid_reg_dict.keys():
                            acid_reg_dict[value].append([float(alt_line_list[iv]),float(lat_line_list[iv]),float(lon_line_list[iv])])
                        else:
                            acid_reg_dict[value]=[]
                            acid_reg_dict[value].append([float(alt_line_list[iv]),float(lat_line_list[iv]),float(lon_line_list[iv])])

                flight_levels_dict={}
                for j in range(30,510,30):
                    flight_levels_dict[j]=0
                flight_levels_list=flight_levels_dict.keys()

                env2_list=[]
                length=0

                for acid in    acid_reg_dict.keys():
                    env2_tmp=0
                    for j in range(len(acid_reg_dict[acid])-1):
                        alt1=acid_reg_dict[acid][j][0]
                        lat1=acid_reg_dict[acid][j][1]
                        lon1=acid_reg_dict[acid][j][2]
                        alt2=acid_reg_dict[acid][j+1][0]
                        lat2=acid_reg_dict[acid][j+1][1]
                        lon2=acid_reg_dict[acid][j+1][2]
                        l=util_functions.get_coordinates_distance(lat1, lon1, lat2, lon2)
                        length+=l
                        alt=(util_functions.convert_feet_to_meters(alt1)+util_functions.convert_feet_to_meters(alt2))/2
                        env2_tmp+=alt*l
                        dict_index=int((alt1+alt2)/(2*30)+0.5)+1
                        if dict_index<0:
                            dict_index=0
                        if dict_index>len(flight_levels_list)-1:
                            dict_index=len(flight_levels_list)-1
                        flight_levels_dict[dict_index*30]+=l
                        

                    env2_list.append(env2_tmp)                               

                env4_values=list(flight_levels_dict.values())
                
                env4=(max(env4_values)-min(env4_values))/(length/len(flight_levels_list))
                env2= sum(env2_list)/length

                tmp_list = [ scenario_name, env2,env4]
                env_mertics_list.append(tmp_list) 

        
        
        env_emtrics_data_frame = pd.DataFrame(env_mertics_list, columns=col_list)

# =============================================================================
#         pd.set_option('display.max_columns', 7)
# 
#         print(reglog_data_frame.head(50))
# =============================================================================
        print("ENV_MERTICS Dataframe created!")
        
        
        output_file=open("dills/env_metrics_dataframe.dill", 'wb')
        dill.dump(env_emtrics_data_frame,output_file)
        output_file.close()

        return env_emtrics_data_frame    
    
##REGLOG dataframe
    def create_reglog_dataframe(self):
        
        col_list = [ "scenario_name", "Time_stamp", "ACID", "ALT", "LAT", "LON"]
        
        reglog_list = list()
        
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

            
        
                for i, line in enumerate(acid_lines_list):
                    acid_line_list = line.split(",")
                     
                    ##The next lines where in the outer intention
                    if i>len(lon_lines_list)-1:
                        print('Problem with',file_name)
                        continue
                    alt_line_list = alt_lines_list[i].split(",")
                    lat_line_list = lat_lines_list[i].split(",")
                    lon_line_list = lon_lines_list[i].split(",")
            
                    for iv, value in enumerate(acid_line_list):
                        if iv == 0:
                            continue
            
                        tmp_list = [ scenario_name, float(acid_line_list[0])]
                 
                        tmp_list.append(value)
                        tmp_list.append(alt_line_list[iv])
                        tmp_list.append(lat_line_list[iv])
                        tmp_list.append(lon_line_list[iv])
            
                 
            
                        reglog_list.append(tmp_list)  # pyspark gives an error when trying to pass list as a value

        
        
        reglog_data_frame = pd.DataFrame(reglog_list, columns=col_list)

# =============================================================================
#         pd.set_option('display.max_columns', 7)
# 
#         print(reglog_data_frame.head(50))
# =============================================================================
        print("REGLOG Dataframe created!")
        
        
        output_file=open("dills/reglog_dataframe.dill", 'wb')
        dill.dump(reglog_data_frame,output_file)
        output_file.close()

        return reglog_data_frame
    
    
    ##DENSITY dataframe
    def create_density_dataframe(self):
        
        col_list = [ "scenario_name", "Time_stamp", "Density"]
        
        densitylog_list = list()
        
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

            
        
                for i, line in enumerate(acid_lines_list):
                    acid_line_list = line.split(",")
                     
                    tmp_list = [ scenario_name, float(acid_line_list[0]),len(acid_line_list)-1]
            
                    densitylog_list.append(tmp_list)  

        
        
        desnistylog_data_frame = pd.DataFrame(densitylog_list, columns=col_list)

# =============================================================================
#         pd.set_option('display.max_columns', 7)
# 
#         print(reglog_data_frame.head(50))
# =============================================================================
        print("DensityLOG Dataframe created!")
        print(desnistylog_data_frame.count())
        
        
        output_file=open("dills/densitylog_dataframe.dill", 'wb')
        dill.dump(desnistylog_data_frame,output_file)
        output_file.close()

        return desnistylog_data_frame

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
        col_list = ["Scenario_name","SAF1", "SAF2", "SAF2_1", "SAF3","SAF4", "SAF5", "SAF6", "SAF6_1","SAF6_2","SAF6_3","SAF6_4","SAF6_5","SAF6_6","SAF6_7" ]
            
 
        dataframe_cnt=0
        
        input_file=open("dills/geolog_dataframe.dill", 'wb')
        geo_log_dataframe=dill.load(input_file)
        input_file.close()
        input_file=open("dills/loslog_dataframe.dill", 'wb')
        los_log_dataframe=dill.load(input_file)
        input_file.close()
        input_file=open("dills/conflog_dataframe.dill", 'wb')
        conf_log_dataframe=dill.load(input_file)
        input_file.close()

        metrics_list = list
          
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
                
            
                
                filtered_geo_dataframe=geo_log_dataframe[geo_log_dataframe["scenario_name"]==scenario_name]
                filtered_los_dataframe=los_log_dataframe[los_log_dataframe["scenario_name"]==scenario_name] 
                filtered_conf_dataframe=conf_log_dataframe[conf_log_dataframe["scenario_name"]==scenario_name] 
    
            
                saf1=SAF_metrics.compute_saf1(filtered_conf_dataframe) 
                saf2=SAF_metrics.compute_saf2(filtered_los_dataframe) 
                saf2_1=SAF_metrics.compute_saf2_1(filtered_los_dataframe) 
                saf3=(saf1-saf2)/saf2
                saf4=SAF_metrics.compute_saf4(filtered_los_dataframe) 
                saf5=SAF_metrics.compute_saf5(filtered_los_dataframe)
                saf6=SAF_metrics.compute_saf6(filtered_geo_dataframe)
                saf6_1=SAF_metrics.compute_saf6_1(filtered_geo_dataframe)
                saf6_2=SAF_metrics.compute_saf6_2(filtered_geo_dataframe)
                saf6_3=SAF_metrics.compute_saf6_3(filtered_geo_dataframe)
                saf6_4=SAF_metrics.compute_saf6_4(filtered_geo_dataframe)
                saf6_5=SAF_metrics.compute_saf6_5(filtered_geo_dataframe)
                saf6_6=SAF_metrics.compute_saf6_6(filtered_geo_dataframe)
                saf6_7=SAF_metrics.compute_saf6_7(filtered_geo_dataframe)


                tmp_list = [scenario_name, saf1,saf2,saf2_1,saf3,saf4,saf5,saf6,saf6_1,saf6_2,saf6_3,saf6_4,saf6_5,saf6_6,saf6_7]
                    
                
                metrics_list.append(tmp_list)
                

        
        metrics_data_frame = pd.DataFrame(metrics_list, columns=col_list)
        
        del geo_log_dataframe
        del los_log_dataframe
        del conf_log_dataframe
        
        input_file=open("dills/flstlog_dataframe.dill", 'wb')
        flst_log_dataframe=dill.load(input_file)
        input_file.close()
        

        
        
        col_list = ["Scenario_name", "#Aircraft_number","#Succeful_aircraft_number","#Spawned_aircraft_number", "AEQ1", "AEQ1_1", "AEQ2", "AEQ2_1", "AEQ3", "AEQ4", "AEQ5",
                    "AEQ5_1", "CAP1", "ENV1","EFF1", "EFF2", "EFF3", "EFF4", "EFF5", "EFF6",  "PRI1", "PRI2"]        
        
        metrics_list = list
        prio_metrics_list = list()  
        prio_col_list=["PRI3","PRI4","PRI5"]             
        
  
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
                filtered_flst_dataframe=flst_log_dataframe[flst_log_dataframe["scenario_name"]==scenario_name]
                

                aircraft_number=filtered_flst_dataframe.count() 
                aircraft_succesful_number=filtered_flst_dataframe[filtered_flst_dataframe["Mission_completed"]==True].count() 
                aircraft_spawned_number=filtered_flst_dataframe[filtered_flst_dataframe["Spawned"]==True].count()
                aeq1=AEQ_metrics.compute_aeq1(filtered_flst_dataframe) 
                aeq1_1=aeq1/aircraft_number
                aeq2=AEQ_metrics.compute_aeq2(filtered_flst_dataframe)
                aeq2_1=aeq2/aircraft_spawned_number
                aeq3=AEQ_metrics.compute_aeq3(filtered_flst_dataframe)
                aeq4=AEQ_metrics.compute_aeq4(filtered_flst_dataframe)
                aeq5=AEQ_metrics.compute_aeq5(filtered_flst_dataframe)
                aeq5_1=aeq5/aircraft_number
                
                eff1=EFF_metrics.compute_eff1(filtered_flst_dataframe) 
                eff2=EFF_metrics.compute_eff2(filtered_flst_dataframe) 
                eff3=EFF_metrics.compute_eff3(filtered_flst_dataframe) 
                eff4=EFF_metrics.compute_eff4(filtered_flst_dataframe) 
                eff5=EFF_metrics.compute_eff5(filtered_flst_dataframe) 
                eff6=EFF_metrics.compute_eff6(filtered_flst_dataframe) 
                
                pri1=PRI_metrics.compute_pri1(filtered_flst_dataframe) 
                pri2=PRI_metrics.compute_pri2(filtered_flst_dataframe) 
            
                
                cap1=CAP_metrics.compute_cap1(filtered_flst_dataframe) 
                
                
                env1=ENV_metrics.compute_env1(filtered_flst_dataframe)
                

                tmp_list = [scenario_name, aircraft_number,aircraft_succesful_number,aircraft_spawned_number, aeq1,aeq1_1, aeq2, aeq2_1, aeq3,aeq4,aeq5,
                            aeq5_1,cap1,env1,eff1,eff2,eff3,eff4,eff5,eff6,pri1,pri2]
                    
                
                metrics_list.append(tmp_list)
                
                for priority in [1,2,3,4]:
                
                    pri3=PRI_metrics.compute_pri3(filtered_flst_dataframe,priority) 
                    pri4=PRI_metrics.compute_pri4(filtered_flst_dataframe,priority) 
                    pri5=PRI_metrics.compute_pri5(filtered_flst_dataframe,priority) 
                    
                    prio_metrics_list.append([scenario_name,priority,pri3,pri4,pri5])
        
        metrics_data_frame2 = pd.DataFrame(metrics_list, columns=col_list)
        prio_metrics_data_frame = pd.DataFrame(prio_metrics_list, columns=prio_col_list)
        metrics_data_frame=pd.merge(metrics_data_frame,metrics_data_frame2,on=["Scenario_name"],how="outer")
        
        metrics_data_frame["CAP2"]=metrics_data_frame["SAF2"]/metrics_data_frame["#Spawned_aircraft_number"]

        del flst_log_dataframe            
                    
        col_list = ["Scenario_name","CAP3", "CAP4"]
        dataframe_cnt=0
        
        metrics_list = list()
       
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


               cap1_rogue=metrics_data_frame[metrics_data_frame["Scenario_name"]==scenario_name].iloc[0]["CAP1"]
               cap1_determ=metrics_data_frame[metrics_data_frame["Scenario_name"]==deterministic_scenario_name].iloc[0]["CAP1"]
               cap2_rogue=metrics_data_frame[metrics_data_frame["Scenario_name"]==scenario_name].iloc[0]["CAP2"]
               cap2_determ=metrics_data_frame[metrics_data_frame["Scenario_name"]==deterministic_scenario_name].iloc[0]["CAP2"]

          
               cap3=cap1_rogue-cap1_determ
               cap4=cap2_rogue-cap2_determ
               tmp_list = [scenario_name, cap3,cap4]
                    
                
               metrics_list.append(tmp_list)
        
        cap_data_frame = pd.DataFrame(metrics_list, columns=col_list)

    

        metrics_data_frame=pd.merge(metrics_data_frame,cap_data_frame,on=["Scenario_name"],how="outer")
        
        input_file=open("dills/env_metrics_dataframe.dill", 'wb')
        env_metrics_dataframe=dill.load(input_file)
        input_file.close()
        
        metrics_data_frame=pd.merge(metrics_data_frame,env_metrics_dataframe,on=["Scenario_name"],how="outer")
        
        
        print("Metrics dataframes created!")
        
        
         
        output_file=open("dills/metrics_dataframe.dill", 'wb')
        dill.dump(metrics_data_frame,output_file)
        output_file.close()
        
        output_file=open("dills/prio_metrics_dataframe.dill", 'wb')
        dill.dump(prio_metrics_data_frame,output_file)
        output_file.close()
        
        return metrics_data_frame

    ####

 

