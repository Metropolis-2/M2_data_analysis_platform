# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class PRI_metrics():
    
    def __init__(self, *args, **kwargs):
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
        else:
            print("No valid metric")
    
    def compute_PRI1_metric(self):
        print("compute_PRI1_metric")
        PRI1=1
        return "PRI1"
    
    def compute_PRI2_metric(self):
        print("compute_PRI2_metric")
        PRI2=2
        return "PRI2"
    
    def compute_PRI3_metric(self):
        print("compute_PRI3_metric")
        PRI3=3
        return "PRI3"
    
    def compute_PRI4_metric(self):
        print("compute_PRI4_metric")
        PRI4=4
        return "PRI4"
    