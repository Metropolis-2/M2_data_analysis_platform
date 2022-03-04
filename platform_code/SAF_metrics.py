# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class SAF_metrics():
    
    def __init__(self, loslog_dataframe, conflog_dataframe, geolog_dataframe):
        self.loslog_dataframe = loslog_dataframe
        self.conflog_dataframe = conflog_dataframe
        self.geolog_dataframe = geolog_dataframe

        #TODO: What types of conflicts exist apart from loss_separation? Differences between the CONF_LOG and LOSS_LOG files?
        
    def evaluate_SAF_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_SAF1_metric()
        elif(metric_id == 2):
            return self.compute_SAF2_metric()
        elif(metric_id == 3):
            return self.compute_SAF3_metric()
        elif(metric_id == 4):
            return self.compute_SAF4_metric()
        elif(metric_id == 5):
            return self.compute_SAF5_metric()
        elif(metric_id == 6):
            return self.compute_SAF6_metric()
        else:
            print("No valid metric")
    
    def compute_SAF1_metric(self):
        '''
        SAF-1: Number of conflicts
        (Number of aircraft pairs that will experience a loss of separation within the look-ahead time)
        '''
        #TODO: "conflict" is a general conflict of CONFLOG file?
        result = self.conflog_dataframe.select("ACID1", "ACID2").count()
        return result
    
    def compute_SAF2_metric(self):
        '''
        SAF-2: Number of intrusions
        (Number of aircraft pairs that experience loss of separation)
        '''
        # TODO: "intrusion" is a loss_separation copnflcit of LOSLOG file?
        result = self.loslog_dataframe.select("ACID1", "ACID2").count()
        return result
    
    def compute_SAF3_metric(self):
        '''
        SAF-3: Intrusion prevention rate
        (Ratio representing the proportion of conflicts that did not result in a loss of separation)
        '''
        num_conflicts = self.compute_SAF1_metric() #TODO: Is a conflict resulting from a loss of separation an intrusion?
        result = int(num_conflicts) / int(self.loslog_dataframe.select("ACID1", "ACID2").count())
        return result
    
    def compute_SAF4_metric(self):
        '''
        SAF-4: Minimum separation
        (The minimum separation between aircraft during conflicts)
        '''
        #TODO: minimum_separation is "DIST "param of dataframe LOSLOG?
        result = self.loslog_dataframe.select("ACID1", "ACID2", "DIST").show()
        return result

    def compute_SAF5_metric(self):
        '''
        SAF-5: Time spent in LOS
        (Total time spent in a state of intrusion)
        '''
        # TODO: intrusion_time is "Time_of_min_distance" param of dataframe LOSLOG?
        result = self.loslog_dataframe.select("ACID1", "ACID2", "Time_of_min_distance").show()
        return

    def compute_SAF6_metric(self):
        '''
        SAF-6: Geofence violations
        (The number of geofence/building area violations)
        '''
        #TODO: all the lines of GEOLOG are geofence/building area violations?
        result = self.geolog_dataframe.select("ACID").count()
        return result