# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""
import utils

class CAP_metrics():
    
    def __init__(self, fp_intention_dataframe, flst_log_dataframe):
        self.utils = utils.Utils()
        self.fp_intention_dataframe = fp_intention_dataframe
        self.flst_log_dataframe = flst_log_dataframe
        self.rec_snd_pointDict = dict(
            [(str(row["FPLAN_ID_INDEX"]), [eval(row["INITIAL_LOCATION_INDEX"]), eval(row["FINAL_LOCATION_INDEX"])]) for
             row in self.fp_intention_dataframe.select("FPLAN_ID_INDEX", "INITIAL_LOCATION_INDEX", "FINAL_LOCATION_INDEX").collect()])
        return
        
    def evaluate_CAP_metric(self, metric_id,):

        if(metric_id == 1):
            return self.compute_CAP1_metric()
        elif(metric_id == 2):
            return self.compute_CAP2_metric()
        elif(metric_id == 3):
            return self.compute_CAP3_metric()
        elif(metric_id == 4):
            return self.compute_CAP4_metric()
        else:
            print("No valid metric")
    
    def compute_CAP1_metric(self):
        '''
        CAP-1: Average demand delay
        (Measured as an arithmetic mean of delay of all flight intentions,
        where delay for each flight intention is calculated as the difference between realized arrival time
        and ideal expected arrival time.
        Ideal expected arrival time is computed as arrival time of the fastest trajectory
        from origin to destination departing at the requested time as if a user were alone in the system,
        respecting all concept airspace rules. Realized arrival time comes directly from the simulation)
        '''
        #Calculate euclidian distance between sending and receiving points for each ACID
        dist_pointDict = {}
        for key, value in self.rec_snd_pointDict.items():
            snd_point = value[0]
            rcv_point = value[1]
            eu_dist = self.utils.calculateDistTwoPoints(snd_point, rcv_point)
            dist_pointDict[key] = eu_dist

        print (dist_pointDict)
        return
    
    def compute_CAP2_metric(self):
        '''
        CAP-2: Average number of intrusions
        (Number of intrusions per flight intention I.e., a ration between total number of intrusions (SAF-2 indicator)
        and number of flight intentions.Intrusions are situations in which the distance between two aircraft is smaller
        than separation norm of 32 metres horizontally and 25 feet vertically, and is directly computed during the simulation)
        '''
        return
    
    def compute_CAP3_metric(self):
        '''
        CAP-3: Additional demand delay
        (Calculated as an increase of the CAP-1 indicator with the introduction of rogue aircraft)
        :return:
        '''
        return
    
    def compute_CAP4_metric(self):
        '''
        CAP-4: Additional number of intrusions
        (Calculated as an increase of the CAP-2 indicator with the introduction of rogue aircraft)
        '''
        return
    