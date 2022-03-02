# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class ENV_metrics():
    
    def __init__(self, flst_log_dataframe):
        self.flst_log_dataframe = flst_log_dataframe
        return
        
    def evaluate_ENV_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_ENV1_metric()
        elif(metric_id == 2):
            return self.compute_ENV2_metric()
        elif(metric_id == 3):
            return self.compute_ENV3_metric()
        elif(metric_id == 4):
            return self.compute_ENV4_metric()
        else:
            print("No valid metric")
    
    def compute_ENV1_metric(self):
        '''
        ENV-1: Work done
        (Representing total energy needed to perform all flight intentions, computed by integrating the thrust (force) over the route displacement.
        The indicator is directly computed in the Bluesky simulator)
        '''
        result = self.flst_log_dataframe.agg({'Work_done': 'sum'}).show()
        return result
    
    def compute_ENV2_metric(self): #TODO: PENDING
        '''
        ENV-2: Weighted average altitude
        (Average flight level weighed by the length flown at each flight level)
        #TODO: How do we know the flight levels of each route? Should we have the ADDWAYPOINTS command from the scenario file (.scn)? In the REGLOG it is mapped every 30sec and it may not be the trajectory change points.
        '''
        return
    
    def compute_ENV3_metric(self): #TODO: PENDING
        '''
        ENV-3: Equivalent Noise Level
        (Represent total sound exposure at the given point on city area surface.
        It is computed by aggregating the total sound intensity (of all sound sources) at that given point over the time)
        '''
        #TODO: Clarify equations (pag.33 D3.1). How to estimate the distance reference with respect to the lowest flight layer?
        return
    
    def compute_ENV4_metric(self): #TODO: PENDING
        '''
        ENV-4: Altitude dispersion
        (The ratio between the difference of maximum and minimum length flown at a flight level and average length flown at level)
        '''
        #TODO: Clarify equations (pag.34 D3.1). What is the unique altitude discretization reference?
        return
    