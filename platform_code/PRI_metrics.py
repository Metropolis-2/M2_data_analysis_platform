# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
import utils
from platform_code.ctes import Ctes


class PRI_metrics():
    
    def __init__(self, flst_log_dataframe):
        self.ctes = Ctes()
        self.utils = utils.Utils()
        self.flst_log_dataframe = flst_log_dataframe

        #Extract list ACIDs of each priority type
        self.prio1_list = [str(row["ACID"]) for row in
                     self.flst_log_dataframe.select("ACID").filter("Priority==1").collect()]
        self.num_prio1 = len(self.prio1_list)
        self.prio2_list = [str(row["ACID"]) for row in
                     self.flst_log_dataframe.select("ACID").filter("Priority==2").collect()]
        self.num_prio2 = len(self.prio2_list)
        self.prio3_list = [str(row["ACID"]) for row in
                     self.flst_log_dataframe.select("ACID").filter("Priority==3").collect()]
        self.num_prio3 = len(self.prio3_list)
        self.prio4_list = [str(row["ACID"]) for row in
                     self.flst_log_dataframe.select("ACID").filter("Priority==4").collect()]
        self.num_prio4 = len(self.prio4_list)
        return
        
    def evaluate_PRI_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_PRI1_metric()
        elif(metric_id == 2):
            return self.compute_PRI2_metric()
        elif(metric_id == 3):
            return self.compute_PRI3_metric()
        elif(metric_id == 4):
            return self.compute_PRI4_metric()
        elif(metric_id == 5):
            return self.compute_PRI5_metric()
        else:
            print("No valid metric")
    
    def compute_PRI1_metric(self):
        '''
        PRI-1: Weighted mission duration
        (Total duration of missions weighted in function of priority level)
        '''

        #Get the flight_time of each vehicle ACID and sum by prio type
        fptimes_prio1 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio1_list)).collect()])
        fptimes_prio2= sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio2_list)).collect()])
        fptimes_prio3 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio3_list)).collect()])
        fptimes_prio4 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio4_list)).collect()])

        result = (self.ctes.WEIGHT_PRIO1*fptimes_prio1) + (self.ctes.WEIGHT_PRIO2*fptimes_prio2) + (self.ctes.WEIGHT_PRIO3*fptimes_prio3) + (self.WEIGHT_PRIO4*fptimes_prio4)
        return result
    
    def compute_PRI2_metric(self):
        '''
        PRI-2: Weighted mission track length
        (Total distance travelled weighted in function of priority level)
        '''

        #TODO: is based on 2D_dist, 3D_dist, Baseline_2d_path_length or Baseline_3d_path_length?
        #Get the distance missions of each ACID and sum by prio type
        fpdist_prio1 = sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio1_list)).collect()])
        fpdist_prio2= sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio2_list)).collect()])
        fpdist_prio3 = sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio3_list)).collect()])
        fpdist_prio4 = sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio4_list)).collect()])

        result = (self.ctes.WEIGHT_PRIO1*fpdist_prio1) + (self.ctes.WEIGHT_PRIO2*fpdist_prio2) + (self.ctes.WEIGHT_PRIO3*fpdist_prio3) + (self.WEIGHT_PRIO4*fpdist_prio4)
        return result
    
    def compute_PRI3_metric(self):
        '''
        PRI-3: Average mission duration per priority level
        (The average mission duration for each priority level per aircraft)
        '''
        #Get the flight_time of each ACID and sum by type
        fptimes_prio1 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe["ACID"]).isin(self.prio1_list)).collect()])
        fptimes_prio2= sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe["ACID"]).isin(self.prio2_list)).collect()])
        fptimes_prio3 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe["ACID"]).isin(self.prio3_list)).collect()])
        fptimes_prio4 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe["ACID"]).isin(self.prio4_list)).collect()])

        #Calculate the flight_time average value with the number of each prio type vehicles
        if(self.num_prio1 >0):
            result_prio1 = fptimes_prio1/self.num_prio1
        else:
            result_prio1 = 0

        if(self.num_prio2>0):
            result_prio2 = fptimes_prio2/self.num_prio2
        else:
            result_prio2 = 0

        if(self.num_prio3 >0):
            result_prio3 = fptimes_prio3/self.num_prio3
        else:
            result_prio3 = 0

        if(self.num_prio4 >0):
            result_prio4 = fptimes_prio4/self.num_prio4
        else:
            result_prio4 = 0

        result = (result_prio1, result_prio2, result_prio3, result_prio4) #TODO: return result, 4 params?
        return result
    
    def compute_PRI4_metric(self):
        '''
        PRI-4: Average mission track length per priority level
        (The average distance travelled for each priority level per aircraft)
        '''
        # TODO: is based on 2D_dist, 3D_dist, Baseline_2d_path_length or Baseline_3d_path_length?
        #Get the distance missions of each ACID and sum by type
        fpdist_prio1 = sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio1_list)).collect()])
        fpdist_prio2= sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio2_list)).collect()])
        fpdist_prio3 = sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio3_list)).collect()])
        fpdist_prio4 = sum([float(row["3D_dist"]) for row in self.flst_log_dataframe.select("3D_dist").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio4_list)).collect()])

        # Calculate the distance missions average value with the number of each prio type vehicles
        if(self.num_prio1 >0):
            result_prio1 = fpdist_prio1/self.num_prio1
        else:
            result_prio1 = 0

        if(self.num_prio2>0):
            result_prio2 = fpdist_prio2/self.num_prio2
        else:
            result_prio2 = 0

        if(self.num_prio3 >0):
            result_prio3 = fpdist_prio3/self.num_prio3
        else:
            result_prio3 = 0

        if(self.num_prio4 >0):
            result_prio4 = fpdist_prio4/self.num_prio4
        else:
            result_prio4 = 0

        result = (result_prio1, result_prio2, result_prio3, result_prio4) #TODO: return result, 4 params?
        return result

    def compute_PRI5_metric(self):
        '''
        PRI-5: Total delay per priority level
        (The total delay experienced by aircraft in a certain priority category relative to ideal conditions)
        '''
        #TODO: With what we know from the fp_intention file and the FLSTLOG log file,
        # we can only calculate the delay that each flight plan mission had for each vehicle and make the sum

        #Extract departure_time of fplanintention of each ACID
        departure_time_dict = dict([(str(row.ACID),[self.utils.get_sec(str(row.Baseline_deparure_time))]) for row in self.flst_log_dataframe.select("ACID", "Baseline_deparure_time").collect()])
        #Extract spawn_time of FLSTLOG of each ACID
        spawn_time_dict = dict([(str(row["ACID"]), [int(row["SPAWN_time"])]) for row in self.flst_log_dataframe.select("ACID","SPAWN_time").collect()])
        print("len(departure_time_dict): {} and values: {}".format(len(departure_time_dict),departure_time_dict))
        print("len(spawn_time_dict): {} and values: {}".format(len(spawn_time_dict),spawn_time_dict))

        # Merge dictionaries and add values of common keys in a list. [key1] = [departure_time1, spawn_time1]
        merged_dict = self.utils.mergeDict(departure_time_dict, spawn_time_dict)
        merged_dict = dict((k, v) for k, v in merged_dict.items() if len(v)>1) #keep only the mission approved (spawned flights has value list >1)
        print('merged_dict: {}'.format(merged_dict))

        delays_dict_prio1 = {} #values in seconds
        delays_dict_prio2 = {}  # values in seconds
        delays_dict_prio3 = {}  # values in seconds
        delays_dict_prio4 = {}  # values in seconds
        total_delay_prio1 = 0 #in seconds
        total_delay_prio2 = 0  # in seconds
        total_delay_prio3 = 0  # in seconds
        total_delay_prio4 = 0  # in seconds
        for k, v in merged_dict.items():
            delay = abs(v[0] - v[1]) #Calculate delay between departure_time and spawn_time
            if(k in self.prio1_list): #acids with prio1
                delays_dict_prio1[k] = delay
                total_delay_prio1 += delay
            elif(k in self.prio1_list): #acids with prio2
                delays_dict_prio2[k] = delay
                total_delay_prio2 += delay
            elif(k in self.prio1_list): #acids with prio3
                delays_dict_prio3[k] = delay
                total_delay_prio3 += delay
            else: #acids with prio4
                delays_dict_prio4[k] = delay
                total_delay_prio4 += delay

        final_total_delay = (total_delay_prio1, total_delay_prio2, total_delay_prio3, total_delay_prio4)
        print("final_total_delay: {}".format(final_total_delay))
        result = final_total_delay #TODO: return result, 4 params?
        return result