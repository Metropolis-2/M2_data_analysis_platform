# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class CAP_metrics():
    
    def __init__(self, *args, **kwargs):
        return
        
    def evaluate_CAP_metric(self, metric_id):
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
        print("compute_CAP1_metric")
        CAP1=1
        return "CAP1"
    
    def compute_CAP2_metric(self):
        print("compute_CAP2_metric")
        CAP2=2
        return "CAP2"
    
    def compute_CAP3_metric(self):
        print("compute_CAP3_metric")
        CAP3=3
        return "CAP3"
    
    def compute_CAP4_metric(self):
        print("compute_CAP4_metric")
        CAP4=4
        return "CAP4"
    