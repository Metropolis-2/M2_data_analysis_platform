# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:49:23 2022

@author: jpedrero
"""

class EFF_metrics():
    
    def __init__(self, *args, **kwargs):
        return
        
    def evaluate_EFF_metric(self, metric_id):
        if(metric_id == 1):
            return self.compute_EFF1_metric()
        elif(metric_id == 2):
            return self.compute_EFF2_metric()
        elif(metric_id == 3):
            return self.compute_EFF3_metric()
        elif(metric_id == 4):
            return self.compute_EFF4_metric()
        elif(metric_id == 5):
            return self.compute_EFF5_metric()
        elif(metric_id == 6):
            return self.compute_EFF6_metric()
        elif(metric_id == 7):
            return self.compute_EFF7_metric()
        elif(metric_id == 8):
            return self.compute_EFF8_metric()
        else:
            print("No valid metric")
    
    def compute_EFF1_metric(self):
        print("compute_EFF1_metric")
        EFF1=1
        return "EFF1"
    
    def compute_EFF2_metric(self):
        print("compute_EFF2_metric")
        EFF2=2
        return "EFF2"
    
    def compute_EFF3_metric(self):
        print("compute_EFF3_metric")
        EFF3=3
        return "EFF3"
    
    def compute_EFF4_metric(self):
        print("compute_EFF4_metric")
        EFF4=4
        return "EFF4"
    
    def compute_EFF5_metric(self):
        print("compute_EFF5_metric")
        EFF5=5
        return "EFF5"

    def compute_EFF6_metric(self):
        print("compute_EFF6_metric")
        EFF6=6
        return "EFF6"
    
    def compute_EFF7_metric(self):
        print("compute_EFF7_metric")
        EFF7=7
        return "EFF7"
    
    def compute_EFF8_metric(self):
        print("compute_EFF8_metric")
        EFF8=8
        return "EFF8"    