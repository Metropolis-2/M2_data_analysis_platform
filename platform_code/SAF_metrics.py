# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class SAF_metrics():
    
    def __init__(self, *args, **kwargs):
        return
        
    def evaluate_SAF_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_SAF1_metric()
        elif(metric_id == 2):
            return self.compute_SAF2_metric()
        elif(metric_id == 3):
            return self.compute_SAF3_metric()
        elif(metric_id == 4):
            return self.compute_SAF4_metric()
        else:
            print("No valid metric")
    
    def compute_SAF1_metric(self):
        print("compute_SAF1_metric")
        SAF1=1
        return "SAF1"
    
    def compute_SAF2_metric(self):
        print("compute_SAF2_metric")
        SAF2=2
        return "SAF2"
    
    def compute_SAF3_metric(self):
        print("compute_SAF3_metric")
        SAF3=3
        return "SAF3"
    
    def compute_SAF4_metric(self):
        print("compute_SAF4_metric")
        SAF4=4
        return "SAF4"
    