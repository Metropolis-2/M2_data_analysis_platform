# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 15:07:06 2022

@author: nipat
"""
import matplotlib.pyplot as plt 
from pyspark.sql import SparkSession
import seaborn as sns
import pandas as pd
import numpy as np

import random ##imported for testing purposes
from matplotlib.patches import PathPatch


def adjust_box_widths(g, fac):
    """
    Adjust the withs of a seaborn-generated boxplot.
    """

    # iterating through Axes instances
    for ax in g.axes:

        # iterating through axes artists:
        for c in ax.get_children():

            # searching for PathPatches
            if isinstance(c, PathPatch):
                # getting current width of box:
                p = c.get_path()
                verts = p.vertices
                verts_sub = verts[:-1]
                xmin = np.min(verts_sub[:, 0])
                xmax = np.max(verts_sub[:, 0])
                xmid = 0.5*(xmin+xmax)
                xhalf = 0.5*(xmax - xmin)

                # setting new width of box
                xmin_new = xmid-fac*xhalf
                xmax_new = xmid+fac*xhalf
                verts_sub[verts_sub[:, 0] == xmin, 0] = xmin_new
                verts_sub[verts_sub[:, 0] == xmax, 0] = xmax_new

                # setting new width of median line
                for l in ax.lines:
                    if np.all(l.get_xdata() == [xmin, xmax]):
                        l.set_xdata([xmin_new, xmax_new])


diagrams_path=""

concepts=["1_","2_","3_"]
concept_names=["Centralised","Hybrid","Decentralised"]
concept_names_dict={}
for i in range(len(concepts)):
    concept_names_dict[concepts[i]]=concept_names[i]

densities=["very_low_","low_","medium_","high_","ultra_"]
density_names=["very_low","low","medium","high","very_high"]
density_names_dict={}
for i in range(len(densities)):
    density_names_dict[densities[i]]=density_names[i]

traffic_mix=["40_","50_","60_"]
traffic_mix_names=["40%","50%","60%"]
traffic_mix_names_dict={}
for i in range(len(traffic_mix)):
    traffic_mix_names_dict[traffic_mix[i]]=traffic_mix_names[i]

repetitions=["0_","1_","2_","3_","4_","5_","6_","7_","8_"]

uncertainties=["","R1","R2","R3","W1","W3","W5"]
rogue_uncertainties=["","R1","R2","R3"]
wind_uncertainties=["","W1","W3","W5"]
uncertainties_names=["No uncertainty","R1","R2","R3","W1","W3","W5"]
uncertainties_names_dict={}
for i in range(len(uncertainties)):
    uncertainties_names_dict[uncertainties[i]]=uncertainties_names[i]

concepts_colours=['r','g','b']

def metric_boxplots_baseline(metric,dataframe):
    vals=[]
    for density in densities:
        for t_mix in traffic_mix:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep
                    try:
                        metric_value=dataframe[dataframe["Scenario"]==scenario_name][metric].values[0]
                    except:
                        metric_value=240+random.randint(-5,5)
                        #print("No value for scenario",scenario_name)
                    
                    tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value]
                    vals.append(tmp)
    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
    
    ##Create one graph for every traffic mix
    for t_mix in traffic_mix_names:
        df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for traffic mix "+t_mix)
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        #plt.savefig(diagrams_path+"boxplots/by_traffic_mix/"+metric+"_"+t_mix)
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" density "+dens)
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         #plt.savefig(diagrams_path+"boxplots/by_density/"+metric+"_"+dens)


def metric_boxplots_wind(metric,dataframe,t_mix):
    vals=[]
    for density in densities:
        for wind in wind_uncertainties:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep+wind
                    try:
                        metric_value=dataframe[dataframe["Scenario"]==scenario_name][metric].values[0]
                    except:
                        metric_value=240+random.randint(-5,5)
                        #print("No value for scenario",scenario_name)
                    
                    tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value]
                    vals.append(tmp)
 
    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Wind level"])
    
    ##Create one graph for every wind level
    for r in wind_uncertainties:
        df1=metric_pandas_df[metric_pandas_df["Wind level"]==uncertainties_names_dict[r]]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for wind level "+uncertainties_names_dict[r])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        #plt.savefig(diagrams_path+"boxplots/winds/by_wind_level/"+metric+"_"+r)
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Wind level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" density "+dens)
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         #plt.savefig(diagrams_path+"boxplots/wind/by_density/"+metric+"_"+dens)
         
def metric_boxplots_rogues(metric,dataframe,t_mix):
    vals=[]
    for density in densities:
        for rogue in rogue_uncertainties:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep+rogue
                    try:
                        metric_value=dataframe[dataframe["Scenario"]==scenario_name][metric].values[0]
                    except:
                        metric_value=240+random.randint(-5,5)
                        #print("No value for scenario",scenario_name)
                    
                    tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value]
                    vals.append(tmp)

    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue level"])
    
    ##Create one graph for every rogue level
    for r in rogue_uncertainties:
        df1=metric_pandas_df[metric_pandas_df["Rogue level"]==uncertainties_names_dict[r]]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for rogue level "+uncertainties_names_dict[r])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        #plt.savefig(diagrams_path+"boxplots/rogues/by_rogue_level/"+metric+"_"+r)
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Rogue level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" density "+dens)
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         #plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens)
   
    
def special_metric_boxplots_rogues(metric,dataframe,t_mix):
    #Only for CAP3 and CAP4
    ##TODO should taht be for mutliple traffix mixes as well?
    
    vals=[]
    for density in densities:
        for rogue in rogue_uncertainties[1:]:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep+rogue
                    try:
                        metric_value=dataframe[dataframe["Scenario"]==scenario_name][metric].values[0]
                    except:
                        metric_value=240+random.randint(-5,5)
                        #print("No value for scenario",scenario_name)
                    
                    tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value]
                    vals.append(tmp)

    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric,"Rogue level"])
    
    ##Create one graph for every rogue level
    for r in rogue_uncertainties:
        df1=metric_pandas_df[metric_pandas_df["Rogue level"]==uncertainties_names_dict[r]]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for rogue level "+uncertainties_names_dict[r])
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        #plt.savefig(diagrams_path+"boxplots/rogues/by_rogue_level/"+metric+"_"+r)
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Rogue level', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" density "+dens)
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         #plt.savefig(diagrams_path+"boxplots/rogues/by_density/"+metric+"_"+dens)
   
def metric_boxplots_priority(metric,dataframe,priority):
    ##Only for PRI3, PRI4, PRI5
    vals=[]
    for density in densities:
        for t_mix in traffic_mix:
            for conc in concepts:
                for rep in repetitions:
                    scenario_name=conc+density+t_mix+rep
                    try:
                        metric_value=dataframe[(dataframe["Scenario"]==scenario_name)&(dataframe["Priority"]==priority)][metric].values[0]
                    except:
                        metric_value=240+random.randint(-5,5)
                        #print("No value for scenario",scenario_name)
                    
                    tmp=[concept_names_dict[conc],density_names_dict[density],traffic_mix_names_dict[t_mix],rep,metric_value]
                    vals.append(tmp)

    
    metric_pandas_df=pd.DataFrame(vals,columns=["Concept","Density","Traffic mix","repetition",metric])
    
    ##Create one graph for every traffic mix
    for t_mix in traffic_mix_names:
        df1=metric_pandas_df[metric_pandas_df["Traffic mix"]==t_mix]
        fig=plt.figure()
        sns.boxplot(y=metric, x='Density', data=df1, palette=concepts_colours,hue='Concept').set(title=metric+" for traffic mix "+t_mix+" for priority "+priority)
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        adjust_box_widths(fig, 0.5)
        #plt.savefig(diagrams_path+"boxplots/priority/by_traffic_mix/"+metric+"_"+t_mix)
        
    ##Create one graph for every density
    for dens in density_names:
         df1=metric_pandas_df[metric_pandas_df["Density"]==dens]
         fig=plt.figure()
         sns.boxplot(y=metric, x='Traffic mix', data=df1, palette=concepts_colours,hue='Concept',width=0.7).set(title=metric+" density "+dens+" for priority "+priority)
         plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
         adjust_box_widths(fig, 0.5)
         #plt.savefig(diagrams_path+"boxplots/priority/by_density/"+metric+"_"+dens)      
    
def density_graph(density, t_mix,rep,dataframe):
    vals=[]
    for conc in concepts:
       scenario_name=conc+density+t_mix+rep
       data=[]
       time_stamp=0
       while time_stamp< 10800:
           time_stamp+=30
           try:
               metric_value=dataframe[(dataframe["Scenario"]==scenario_name)&(dataframe["Time_stamp"]==time_stamp)]["Alive_aircraft"].values[0]
           except:
               metric_value=240+random.randint(-5,5)
               #print("No value for scenario",scenario_name)
      
           vals.append(metric_value)

               
       vals.append(data)
       
    time_stamps=range(30,10830,30)
    
    plt.plot(time_stamps, vals[0],   label = 'Centralised', marker='o',color=concepts_colours[0], linewidth=3)
    plt.plot(time_stamps, vals[1],   label = 'Hyrbid',  marker='o',color=concepts_colours[1], linewidth=3)
    plt.plot(time_stamps, vals[2], label = 'Decentralised', marker='o',color=concepts_colours[2], linewidth=3)
    
    plt.xlabel('Time')
    plt.ylabel('Traffic density')
    plt.legend(loc='upper left')
    #plt.xticks(monthList)
    #plt.yticks([1000, 2000, 4000, 6000, 8000, 10000, 12000, 15000, 18000])
    plt.title('Aircraft density for density '+density_names_dict[density])
    plt.show()
       
       
       
       

##Load the metrics
spark = SparkSession.builder.getOrCreate()

scenario_metrics_df=spark.read.parquet("../output/scenario_results.parquet")

#scenario_metrics_df.show()


scenario_priority_metrics_df=spark.read.parquet("../output/scenario_priority_results.parquet")
#scenario_priority_metrics_df.show()


#scenario_timestep_metrics_df=spark.read.parquet("../output/scenario_timestep_results.parquet")
#scenario_priority_metrics_df.show()


## Create the graphs
boxplot_metrics=["AEQ-1","AEQ-1_1","AEQ-2","AEQ-2_1","AEQ-3","AEQ-4","AEQ-5","AEQ-5_1","CAP-1","CAP-2","EFF-1","EFF-2"\
                 "EFF-3","EFF-4","EFF-5","EFF-6","ENV-1","ENV-2","ENV-3","SAF-1","SAF-2","SAF-2_1","SAF-3","SAF-4","SAF-5","SAF-6"\
                     "SAF-6_1","PRI-1","PRI-2"]

boxplot_metrics_rogues=["CAP-3","CAP-4"]

boxplot_metrics_priority=["PRI-3","PRI-4","PRI-5"]


#metric_boxplots_baseline("AEQ-1",scenario_metrics_df)
#metric_boxplots_rogues("AEQ-1",scenario_metrics_df,"40_")
#metric_boxplots_wind("AEQ-1",scenario_metrics_df,"40_")

# =============================================================================
# for metric in boxplot_metrics:
#     metric_boxplots_baseline(metric,scenario_metrics_df)
#     metric_boxplots_rogues("AEQ-1",scenario_metrics_df,"40_")
#     metric_boxplots_wind("AEQ-1",scenario_metrics_df,"40_")
# =============================================================================


# =============================================================================
# for metric in boxplot_metrics_rogues:
#     special_metric_boxplots_rogues(metric,scenario_metrics_df,"40_")
# =============================================================================

# =============================================================================
# for metric in boxplot_metrics_priority:
#     for priority in ["1","2","3","4"]:
#         metric_boxplots_priority(metric,scenario_priority_metrics_df,priority)
# =============================================================================


# =============================================================================
# t_mix="40_"
# rep="0"
# for dens in densities:
#     density_graph(dens, t_mix,rep,dataframe)
# =============================================================================
    
