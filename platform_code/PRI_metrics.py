# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
import utils

class PRI_metrics():
    
    def __init__(self):
        self.prio1_list = None
        self.prio2_list = None
        self.prio3_list = None
        self.prio4_list = None
        self.num_prio1 = 0
        self.num_prio2 = 0
        self.num_prio3 = 0
        self.num_prio4 = 0
        return
        
    def evaluate_PRI_metric(self, metric_id, scn_dataframe, flst_log_dataframe):
        self.scn_dataframe = scn_dataframe
        self.flst_log_dataframe = flst_log_dataframe
        self.utils = utils.Utils()

        #Extract list ACIDs of each priority type
        self.prio1_list = [str(row.FPLAN_ID_INDEX) for row in
                     self.scn_dataframe.select("FPLAN_ID_INDEX").filter("PRIORITY_INDEX==1").collect()]
        self.num_prio1 = len(self.prio1_list)
        self.prio2_list = [str(row.FPLAN_ID_INDEX) for row in
                     self.scn_dataframe.select("FPLAN_ID_INDEX").filter("PRIORITY_INDEX==2").collect()]
        self.num_prio2 = len(self.prio2_list)
        self.prio3_list = [str(row.FPLAN_ID_INDEX) for row in
                     self.scn_dataframe.select("FPLAN_ID_INDEX").filter("PRIORITY_INDEX==3").collect()]
        self.num_prio3 = len(self.prio3_list)
        self.prio4_list = [str(row.FPLAN_ID_INDEX) for row in
                     self.scn_dataframe.select("FPLAN_ID_INDEX").filter("PRIORITY_INDEX==4").collect()]
        self.num_prio4 = len(self.prio4_list)

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

        #Get the flight_time of each ACID and sum by prio type
        fptimes_prio1 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio1_list)).collect()])
        fptimes_prio2= sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio2_list)).collect()])
        fptimes_prio3 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio3_list)).collect()])
        fptimes_prio4 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio4_list)).collect()])

        result = 1*fptimes_prio1 + 2*fptimes_prio2 + 3*fptimes_prio3 + 4*fptimes_prio4
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

        result = 1*fpdist_prio1 + 2*fpdist_prio2 + 3*fpdist_prio3 + 4*fpdist_prio4
        return result
    
    def compute_PRI3_metric(self):
        '''
        PRI-3: Average mission duration per priority level
        (The average mission duration for each priority level per aircraft)
        '''
        #Get the flight_time of each ACID and sum by type
        fptimes_prio1 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio1_list)).collect()])
        fptimes_prio2= sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio2_list)).collect()])
        fptimes_prio3 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio3_list)).collect()])
        fptimes_prio4 = sum([float(row["FLIGHT_time"]) for row in self.flst_log_dataframe.select("FLIGHT_time").filter(
            (self.flst_log_dataframe.ACID).isin(self.prio4_list)).collect()])

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

        result = (result_prio1, result_prio2, result_prio3, result_prio4)
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

        result = (result_prio1, result_prio2, result_prio3, result_prio4)
        return result

    def compute_PRI5_metric(self):
        '''
        PRI-5: Total delay per priority level
        (The total delay experienced by aircraft in a certain priority category relative to ideal conditions)
        '''
        #TODO: With what we know from the fp_intention file and the FLSTLOG log file,
        # we can only calculate the delay that each flight plan mission had for each vehicle and make the sum

        #Extract departure_time of fplanintention of each ACID
        departure_time_dict = dict([(str(row.FPLAN_ID_INDEX),[self.utils.get_sec(str(row.DEPARTURE_INDEX))]) for row in self.scn_dataframe.select("FPLAN_ID_INDEX", "DEPARTURE_INDEX").collect()])
        #Extract spawn_time of FLSTLOG of each ACID
        spawn_time_dict = dict([(str(row["ACID"]), [int(row["SPAWN_time"])]) for row in self.flst_log_dataframe.select("ACID","SPAWN_time").collect()])
        print("len(departure_time_dict): {} and values: {}".format(len(departure_time_dict),departure_time_dict))
        print("len(spawn_time_dict): {} and values: {}".format(len(spawn_time_dict),spawn_time_dict))

        # Merge dictionaries and add values of common keys in a list
        merged_dict = self.utils.mergeDict(departure_time_dict, spawn_time_dict)
        merged_dict = dict((k, v) for k, v in merged_dict.items() if len(v)>1) #keep only the mission approved (spawned flights has value list >1)
        print('merged_dict: {}'.format(merged_dict))

        delays_dict = {} #values in seconds
        total_delay = 0 #in seconds
        for k, v in merged_dict.items():
            delay = abs(v[0] - v[1]) #Calculate delay between departure_time and spawn_time
            delays_dict[k] = delay
            total_delay += delay
        print("delays_dict: {}".format(delays_dict))
        print("total_delay: {}".format(total_delay))

        result = total_delay #TODO: total delay in sec or hh:mm:ss?
        return result