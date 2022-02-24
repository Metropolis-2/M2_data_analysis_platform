# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class ENV_metrics():
    
    def __init__(self, *args, **kwargs):
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
        print("compute_ENV1_metric")
        ENV1=1
        return "ENV1"
    
    def compute_ENV2_metric(self):
        print("compute_ENV2_metric")
        ENV2=2
        return "ENV2"
    
    def compute_ENV3_metric(self):
        print("compute_ENV3_metric")
        ENV3=3
        return "ENV3"
    
    def compute_ENV4_metric(self):
        print("compute_ENV4_metric")
        ENV4=4
        return "ENV4"
    