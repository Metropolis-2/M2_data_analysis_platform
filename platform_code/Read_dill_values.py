# -*- coding: utf-8 -*-
"""
Created on Tue May 24 15:42:46 2022

@author: nipat
"""
import dill
import pandas
dills_path="dills/"
boxplot_metrics=["AEQ1","AEQ1_1","AEQ2","AEQ2_1","AEQ3","AEQ4","AEQ5","AEQ5_1","CAP1","CAP2","EFF1","EFF2","EFF3","EFF4","EFF5","EFF6","ENV1",\
                         "ENV2","ENV3_1","ENV3_2","ENV4","SAF1","SAF1_2","SAF1_3","SAF1_4","SAF2","SAF2_1","SAF2_2","SAF2_3","SAF3","SAF4","SAF5","SAF5_1","SAF6","SAF6_1","SAF6_2","SAF6_3","SAF6_4","SAF6_5",\
                             "SAF6_6","SAF6_7","PRI1","PRI2","CAP3","CAP4"]
    
    
input_file=open(dills_path+"metrics_dataframe.dill", 'rb')
scenario_metrics_df=dill.load(input_file)
input_file.close()
     
concepts=["1_","2_","3_"]
concept_names=["Centralised","Hybrid","Decentralised"]


##Initialisation of the density types to be graphed
#If you do not want to graph for all five density types, you may delete the unwanted densities from the variables  densities and density_names
densities=["very_low_","low_","medium_","high_","ultra_"]
density_names=["very low","low","medium","high","very high"]

##Initialisation of the traffic mix types to be graphed
#If you do not want to graph for all three traffic mix types, you may delete the unwanted traffic mix from the variables  traffic_mix and traffic_mix_names
traffic_mix=["40_","50_","60_"]
traffic_mix_names=["40%","50%","60%"]

##Initialisation of the repetition number to be graphed
#If you do not want to graph for all nine repetitions, you may delete the unwanted repetitions from the variable repetitions
repetitions=["0_","1_","2_","3_","4_","5_","6_","7_","8_"]

##Initialisation of the uncertaity type to be graphed
#If you do not want to graph for all seven uncertainy types, you may delete the unwanted uncertainty types from the variables uncertainties,rogue_uncertainties,wind_uncertainties and uncertainties_names
uncertainties=["","R1","R2","R3","W1","W3","W5"]


scenario_name="1_very_low_40_0_"
#scenario_name=conc+density+t_mix+rep+uncertainty

filterd_dataframe=scenario_metrics_df[scenario_metrics_df["Scenario_name"]==scenario_name]

metric_value=filterd_dataframe["AEQ1"].values[0]
print(metric_value)

priority_metrics=["PRI3","PRI4","PRI5"]
input_file=open(dills_path+"prio_metrics_dataframe.dill", 'rb')
scenario_priority_metrics_df=dill.load(input_file)
input_file.close()

filterd_dataframe=scenario_priority_metrics_df[scenario_priority_metrics_df["Scenario_name"]==scenario_name]

priority="1"

filterd_dataframe=filterd_dataframe[filterd_dataframe["Priority"]==[priority]]

metric_value=filterd_dataframe["AEQ1"].values[0]
print(metric_value)