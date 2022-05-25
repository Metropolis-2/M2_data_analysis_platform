    # -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 16:33:17 2022
@author: nipat
"""
import os
import pandas as pd2

from datetime import datetime
import DataframeCreator
import GraphCreator

class MainClass():
        def __init__(self,x):

            self.dataframe_creator = DataframeCreator.DataframeCreator(x) # the dataframes are created in  the init function now


if __name__ == "__main__":
    
        choice = input("To generate dataframes enter 1, to generate graphs enter 2, to compute and print noise related statistics enter 3 \n")
        choice = int(choice)
        if choice==1:
            choice = input("Enter number of threads to use\n")
            choice = int(choice)
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            print("Metrics started =", current_time)
            #m=MainClass(choice)
            dataframe_creator = DataframeCreator.DataframeCreator(choice)
            dataframe_creator.create_dataframes()
            now = datetime.now()
    
            current_time = now.strftime("%H:%M:%S")
            print("Metrics ended =", current_time)        
        elif choice==2:
            graphCreator=GraphCreator.GraphCreator()
            graphCreator.createGraphs()
            print("Graphs are being generated ...\n")
            
        elif choice==3:
            choice = input("Enter number of threads to use\n")
            choice = int(choice)
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            print("env stats started =", current_time)
            #m=MainClass(choice)
            dataframe_creator = DataframeCreator.DataframeCreator(choice)
            dataframe_creator.compute_env3_statistics()
            now = datetime.now()
    
            current_time = now.strftime("%H:%M:%S")
            print("env stats ended =", current_time)    


            
