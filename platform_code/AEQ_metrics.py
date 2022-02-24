# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class AEQ_metrics():
    
    def __init__(self, *args, **kwargs):
        return
        
    def evaluate_AEQ_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_AEQ1_metric()
        elif(metric_id == 2):
            return self.compute_AEQ1_1_metric()
        elif(metric_id == 3):
            return self.compute_AEQ2_metric()
        elif(metric_id == 4):
            return self.compute_AEQ2_1_metric()
        elif(metric_id == 5):
            return self.compute_AEQ3_metric()
        elif(metric_id == 6):
            return self.compute_AEQ4_metric()
        elif(metric_id == 7):
            return self.compute_AEQ5_metric()
        elif(metric_id == 8):
            return self.compute_AEQ5_1_metric()
        else:
            print("No valid metric")
    
    def compute_AEQ1_metric(self):
        print("compute_AEQ1_metric")
        AEQ1=1
        return "AEQ1"
    
    def compute_AEQ1_1_metric(self):
        print("compute_AEQ1_1_metric")
        AEQ1_1=11
        return "AEQ1_1"
    
    def compute_AEQ2_metric(self):
        print("compute_AEQ2_metric")
        AEQ2=2
        return "AEQ2"
    
    def compute_AEQ2_1_metric(self):
        print("compute_AEQ2_1_metric")
        AEQ2_1=21
        return "AEQ2_1"
    
    def compute_AEQ3_metric(self):
        print("compute_AEQ3_metric")
        AEQ3=3
        return "AEQ3"
    
    def compute_AEQ4_metric(self):
        print("compute_AEQ4_metric")
        AEQ4=4
        return "AEQ4"
    
    def compute_AEQ5_metric(self):
        print("compute_AEQ5_metric")
        AEQ5=5
        return "AEQ5"
    
    def compute_AEQ5_1_metric(self):
        print("compute_AEQ5_1_metric")
        AEQ5_1=51
        return "AEQ5_1"